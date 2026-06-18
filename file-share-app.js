"use strict";

/*
 * Browser-to-browser file sharing over WebRTC, using PeerJS for signaling.
 *
 * Each browser registers a friendly name (colour-animal-number) as its PeerJS
 * ID with the free public PeerJS broker. To connect, you type someone else's
 * name — PeerJS handles the WebRTC handshake and opens a direct data channel.
 * Files then flow peer-to-peer; the broker never sees them.
 *
 * The interface is symmetric: either side can type the other's name, and either
 * side can send files once connected.
 *
 * To stop relying on the public broker, pass your own server to `new Peer(id, {
 * host, port, path, secure })` — the rest of the code is unchanged.
 */

const CHUNK_SIZE = 16 * 1024;             // 16 KB per chunk
const BUFFER_LIMIT = 8 * 1024 * 1024;     // pause sending above ~8 MB queued

// connection-monitor tuning
const CONNECT_TIMEOUT = 15000;            // give up dialing a peer after 15 s
const HEARTBEAT_INTERVAL = 3000;          // send a ping every 3 s
const HEARTBEAT_TIMEOUT = 10000;          // no traffic for 10 s => connection lost
const BROKER_RETRY_MAX = 6;               // max broker-reconnect attempts
const PEER_RETRY_MAX = 4;                 // max peer-redial attempts
const STABLE_AFTER = 30000;               // connection must last 30 s to count as "stable"
const RATE_WINDOW = 60000;                // rolling window for broker-signaling attempts
const RATE_MAX = 8;                       // max broker-signaling attempts per window
const RATE_COOLDOWN = 60000;              // pause this long once the window cap is hit

let peer = null;          // PeerJS Peer
let conn = null;          // active DataConnection
let incoming = null;      // file currently being received
let myName = "";
let peerName = "";

// monitor state
let initiatedTarget = null;   // name we dialed (so we can redial); null if we were dialed
let connectTimer = null;      // pending-connection timeout
let heartbeatTimer = null;    // ping sender
let watchdogTimer = null;     // silence detector
let lastSeen = 0;             // timestamp of last data from peer
let brokerAttempts = 0;       // broker reconnect counter
let peerAttempts = 0;         // peer redial counter
let redialPending = false;    // guards against overlapping redials
let stableTimer = null;       // fires once a connection has lasted STABLE_AFTER
let signalTimes = [];         // timestamps of recent broker-signaling attempts
let cooldownUntil = 0;        // no broker signaling permitted before this time

// ---------- name generation ----------
const COLOURS = ["red", "blue", "green", "amber", "violet", "coral", "teal",
  "indigo", "olive", "crimson", "azure", "lime", "magenta", "silver", "gold"];
const ANIMALS = ["walrus", "otter", "lynx", "falcon", "panda", "gecko", "moose",
  "heron", "marten", "ibex", "tapir", "narwhal", "badger", "weasel", "raven"];

function generateName() {
  const c = COLOURS[Math.floor(Math.random() * COLOURS.length)];
  const a = ANIMALS[Math.floor(Math.random() * ANIMALS.length)];
  const n = Math.floor(Math.random() * 100); // 0–99
  return c + "-" + a + "-" + n;
}

// ---------- DOM helpers ----------
const $ = (id) => document.getElementById(id);
const setStatus = (msg) => { $("status").textContent = msg; };

// ---------- start PeerJS ----------
function startPeer() {
  myName = generateName();
  peer = new Peer(myName);

  peer.on("open", (id) => {
    brokerAttempts = 0;            // healthy registration resets the counter
    $("myName").textContent = id;
    $("btnConnect").disabled = false;
    setStatus("Ready. Share your name or enter someone else's.");
  });

  // Someone connected to us.
  peer.on("connection", (connection) => {
    conn = connection;
    initiatedTarget = null;        // they dialed us; we can't redial them
    setupConnection();
  });

  // Lost the link to the broker — try to re-register with backoff.
  peer.on("disconnected", () => {
    setStatus("Lost connection to broker — reconnecting…");
    attemptBrokerReconnect();
  });

  peer.on("error", (err) => {
    // If our random name was already taken, pick another and retry.
    if (err.type === "unavailable-id") {
      peer.destroy();
      startPeer();
      return;
    }
    if (err.type === "peer-unavailable") {
      setStatus("No one is online with that name. Check the spelling and try again.");
      return;
    }
    // Network/server errors: let the disconnected handler drive reconnection.
    if (err.type === "network" || err.type === "server-error") {
      setStatus("Broker problem (" + err.type + ") — will keep retrying…");
      return;
    }
    setStatus("Error: " + err.type);
  });
}

// ---------- broker rate-limit guard ----------
// Every call that contacts the broker (peer.connect / peer.reconnect) must pass
// canSignal() first and call recordSignal() when it actually fires. This bounds
// broker contact to RATE_MAX per RATE_WINDOW regardless of how often the
// connection flaps, entering a cooldown if the cap is hit.
function canSignal() {
  const now = Date.now();
  if (now < cooldownUntil) return false;
  signalTimes = signalTimes.filter((t) => now - t < RATE_WINDOW);
  if (signalTimes.length >= RATE_MAX) {
    cooldownUntil = now + RATE_COOLDOWN;
    return false;
  }
  return true;
}
function recordSignal() { signalTimes.push(Date.now()); }
function cooldownSeconds() { return Math.max(1, Math.ceil((cooldownUntil - Date.now()) / 1000)); }

// Re-register with the broker using exponential backoff.
function attemptBrokerReconnect() {
  if (!peer || peer.destroyed) return;

  // Respect the rate guard before counting an attempt.
  if (!canSignal()) {
    setStatus("Pausing " + cooldownSeconds() + "s before retrying the broker (rate-limit guard)…");
    setTimeout(attemptBrokerReconnect, Math.max(cooldownUntil - Date.now(), 1000));
    return;
  }
  if (brokerAttempts >= BROKER_RETRY_MAX) {
    setStatus("Could not reconnect to the broker. Reload the page to retry.");
    return;
  }
  brokerAttempts++;
  const delay = Math.min(1000 * 2 ** (brokerAttempts - 1), 15000);
  setStatus("Reconnecting to broker (attempt " + brokerAttempts + ")…");
  setTimeout(() => {
    if (!peer || peer.destroyed) return;
    if (peer.disconnected) {
      recordSignal();
      try { peer.reconnect(); } catch (_) {}
      // If still down shortly after, the disconnected event fires again and
      // this function is re-entered, continuing the backoff.
      setTimeout(() => {
        if (peer && !peer.destroyed && !peer.disconnected) {
          brokerAttempts = 0;
          setStatus("Reconnected to broker.");
        }
      }, 1500);
    } else {
      brokerAttempts = 0;
    }
  }, delay);
}

// ---------- connect to a named peer ----------
$("btnConnect").onclick = () => {
  const target = $("peerName").value.trim();
  if (!target) { setStatus("Type the other person's name first."); return; }
  if (target === myName) { setStatus("That's your own name."); return; }

  peerAttempts = 0;
  redialPending = false;
  dial(target);
};

// Open (or re-open) a data connection to a named peer, guarded by a timeout.
function dial(target) {
  initiatedTarget = target;
  clearTimeout(connectTimer);

  // Rate guard: if we've signaled too often, wait out the cooldown then retry.
  if (!canSignal()) {
    setStatus("Pausing " + cooldownSeconds() + "s before reconnecting (rate-limit guard)…");
    connectTimer = setTimeout(() => dial(target), Math.max(cooldownUntil - Date.now(), 1000));
    return;
  }

  recordSignal();
  setStatus("Connecting to " + target + "…");
  conn = peer.connect(target);
  setupConnection();

  connectTimer = setTimeout(() => {
    if (!conn || !conn.open) {
      setStatus("Couldn't reach " + target +
        " (timed out). They may be offline or behind a restrictive network.");
      try { conn && conn.close(); } catch (_) {}
    }
  }, CONNECT_TIMEOUT);
}

// ---------- shared connection setup ----------
function setupConnection() {
  conn.on("open", () => {
    clearTimeout(connectTimer);
    peerName = conn.peer;
    redialPending = false;
    setStatus("Connected to " + peerName + " — ready to send files.");
    $("transferSection").hidden = false;
    $("btnSend").disabled = false;
    startHeartbeat();

    // Don't credit the redial budget immediately — a connection that dies
    // within STABLE_AFTER is treated as a flap and keeps drawing down the
    // counter. Only a connection that lasts resets it.
    clearTimeout(stableTimer);
    stableTimer = setTimeout(() => { peerAttempts = 0; }, STABLE_AFTER);
  });

  conn.on("data", handleData);

  conn.on("close", () => {
    connectionLost("Disconnected from " + (peerName || "peer") + ".");
  });

  conn.on("error", (err) => {
    connectionLost("Connection error: " + (err.type || err) + ".");
  });
}

// ---------- connection monitor ----------
function startHeartbeat() {
  lastSeen = Date.now();
  stopHeartbeat();
  // Periodically ping the peer so the link stays measurable.
  heartbeatTimer = setInterval(() => {
    if (conn && conn.open) { try { conn.send({ type: "ping" }); } catch (_) {} }
  }, HEARTBEAT_INTERVAL);
  // Independently watch for silence (covers connections that die without a
  // close event, e.g. network drop or sleeping laptop).
  watchdogTimer = setInterval(() => {
    if (Date.now() - lastSeen > HEARTBEAT_TIMEOUT) {
      connectionLost("Lost contact with " + (peerName || "peer") + " (no response).");
    }
  }, HEARTBEAT_INTERVAL);
}

function stopHeartbeat() {
  clearInterval(heartbeatTimer);
  clearInterval(watchdogTimer);
  heartbeatTimer = null;
  watchdogTimer = null;
}

// Called whenever the active connection drops or goes silent.
function connectionLost(message) {
  if (redialPending) return;   // a redial is already in flight; ignore duplicates
  stopHeartbeat();
  clearTimeout(stableTimer);   // connection ended before it counted as stable
  $("btnSend").disabled = true;
  try { conn && conn.close(); } catch (_) {}

  // Only the side that dialed can redial; if we were dialed, just report.
  if (initiatedTarget) {
    attemptPeerRedial(message);
  } else {
    setStatus(message);
  }
}

function attemptPeerRedial(message) {
  if (peerAttempts >= PEER_RETRY_MAX) {
    setStatus(message + " Couldn't reconnect — try entering the name again.");
    return;
  }
  redialPending = true;
  peerAttempts++;
  const delay = 2000 * peerAttempts;
  setStatus(message + " Reconnecting to " + initiatedTarget +
    " (attempt " + peerAttempts + ")…");
  setTimeout(() => {
    redialPending = false;
    if (peer && !peer.destroyed) dial(initiatedTarget);
  }, delay);
}

// ---------- receiving ----------
function handleData(data) {
  lastSeen = Date.now();   // any traffic counts as the peer being alive

  // Heartbeat: reply to pings, ignore pongs.
  if (data && data.type === "ping") {
    if (conn && conn.open) { try { conn.send({ type: "pong" }); } catch (_) {} }
    return;
  }
  if (data && data.type === "pong") return;

  // Control messages are plain objects; file data arrives as binary.
  if (data && data.type === "meta") {
    incoming = { name: data.name, size: data.size, mime: data.mime, chunks: [] };
    return;
  }
  if (data && data.type === "done") {
    finishIncoming();
    return;
  }

  // Binary chunk — PeerJS may hand back an ArrayBuffer or a typed array.
  const buf = data instanceof ArrayBuffer ? data : (data.buffer || data);
  if (incoming) {
    incoming.chunks.push(buf);
  }
}

function finishIncoming() {
  const blob = new Blob(incoming.chunks, { type: incoming.mime || "application/octet-stream" });
  const url = URL.createObjectURL(blob);

  const li = document.createElement("li");
  const info = document.createElement("span");
  info.textContent =
    incoming.name + " — " + formatBytes(incoming.size) +
    (incoming.mime ? " (" + incoming.mime + ") " : " ") +
    (peerName ? "from " + peerName + " " : "");
  li.appendChild(info);

  const link = document.createElement("a");
  link.href = url;
  link.download = incoming.name;
  link.textContent = "Download";
  li.appendChild(link);

  $("received").appendChild(li);
  incoming = null;
}

// ---------- sending ----------
async function sendFile(file) {
  if (!conn || !conn.open) { setStatus("Not connected yet."); return; }

  conn.send({ type: "meta", name: file.name, size: file.size, mime: file.type });

  let offset = 0;
  while (offset < file.size) {
    // Simple backpressure: wait if PeerJS's send buffer is large.
    while (conn.bufferSize > BUFFER_LIMIT) {
      await new Promise((r) => setTimeout(r, 50));
    }
    const buffer = await file.slice(offset, offset + CHUNK_SIZE).arrayBuffer();
    conn.send(buffer);
    offset += buffer.byteLength;
    $("sendProgress").textContent =
      "Sending " + file.name + ": " + Math.round((offset / file.size) * 100) + "%";
  }

  conn.send({ type: "done" });
  $("sendProgress").textContent = "Sent " + file.name + " (" + formatBytes(file.size) + ").";
}

// ---------- misc ----------
function formatBytes(bytes) {
  if (bytes < 1024) return bytes + " B";
  const units = ["KB", "MB", "GB"];
  let value = bytes / 1024, i = 0;
  while (value >= 1024 && i < units.length - 1) { value /= 1024; i++; }
  return value.toFixed(1) + " " + units[i];
}

$("btnSend").onclick = () => {
  const file = $("fileInput").files[0];
  if (!file) { setStatus("Choose a file first."); return; }
  sendFile(file);
};

// ---------- init ----------
startPeer();
