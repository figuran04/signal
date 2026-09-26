import { spawn } from "node:child_process";
import { setTimeout as delay } from "node:timers/promises";
import process from "node:process";
import WebSocket from "ws";

// Set SIGNAL_WS_URL to run against a deployed Worker instead of a local dev server.
const REMOTE = process.env.SIGNAL_WS_URL;
const PORT = Number(process.env.PORT || 8787);
const URL = REMOTE || `ws://127.0.0.1:${PORT}/`;
const HEALTH = REMOTE
  ? `${REMOTE.replace(/^ws/, "http").replace(/\/+$/, "")}/health`
  : `http://127.0.0.1:${PORT}/health`;

let failures = 0;

const log = (...a) => console.log(...a);
function ok(cond, label, extra) {
  if (cond) log(`  PASS  ${label}`);
  else {
    failures++;
    log(`  FAIL  ${label}`, extra === undefined ? "" : JSON.stringify(extra));
  }
}

async function startDev() {
  const proc = spawn(
    process.execPath,
    ["node_modules/wrangler/bin/wrangler.js", "dev", "--port", String(PORT), "--ip", "127.0.0.1"],
    { stdio: ["ignore", "ignore", "inherit"] }
  );

  for (let i = 0; i < 90; i++) {
    await delay(1000);
    if (proc.exitCode !== null) throw new Error("wrangler dev exited before becoming ready");
    try {
      const res = await fetch(HEALTH);
      if (res.ok) return proc;
    } catch {}
  }

  proc.kill();
  throw new Error("wrangler dev did not become ready in 90s");
}

function connect() {
  const ws = new WebSocket(URL);
  ws.queue = [];
  ws.waiters = [];
  ws.errors = [];
  ws.on("message", (raw) => {
    const msg = JSON.parse(raw.toString());
    const i = ws.waiters.findIndex((w) => w.type === msg.type);
    if (i >= 0) {
      const w = ws.waiters.splice(i, 1)[0];
      clearTimeout(w.timer);
      w.resolve(msg);
    } else {
      ws.queue.push(msg);
    }
  });
  return new Promise((resolve) => ws.on("open", () => resolve(ws)));
}

// A deployed Worker adds TLS, edge routing and Durable Object cold starts on
// top of the round trip, so give remote runs more headroom than localhost.
const EXPECT_TIMEOUT = REMOTE ? 15000 : 5000;

function expect(ws, type, ms = EXPECT_TIMEOUT) {
  const found = ws.queue.findIndex((m) => m.type === type);
  if (found >= 0) return Promise.resolve(ws.queue.splice(found, 1)[0]);
  return new Promise((resolve, reject) => {
    const w = { type, resolve };
    w.timer = setTimeout(() => {
      const i = ws.waiters.indexOf(w);
      if (i >= 0) ws.waiters.splice(i, 1);
      reject(new Error(`timeout waiting for "${type}"`));
    }, ms);
    ws.waiters.push(w);
  });
}

function closed(ws, ms = 5000) {
  if (ws.readyState === WebSocket.CLOSED) return Promise.resolve();
  return new Promise((resolve, reject) => {
    ws.once("close", resolve);
    const t = setTimeout(() => reject(new Error("timeout waiting for close")), ms);
    ws.once("close", () => clearTimeout(t));
  });
}

const send = (ws, payload) => ws.send(JSON.stringify(payload));

async function main() {
  let dev = null;
  if (REMOTE) {
    log(`\ntarget: ${REMOTE} (deployed)`);
  } else {
    log(`\nbooting wrangler dev on 127.0.0.1:${PORT} ...`);
    dev = await startDev();
    log("dev server ready");
  }

  try {
    await run();
  } finally {
    dev?.kill();
  }

  log(`\n${failures === 0 ? "ALL TESTS PASSED" : failures + " TEST(S) FAILED"}\n`);
  process.exit(failures === 0 ? 0 : 1);
}

// Cloudflare closes idle keep-alive connections, and undici will happily reuse a
// dead socket -> ECONNRESET. Retry so the harness is not the flaky part.
async function fetchRetry(url, attempts = 3) {
  let last;
  for (let i = 0; i < attempts; i++) {
    try {
      return await fetch(url);
    } catch (err) {
      last = err;
      await delay(500 * (i + 1));
    }
  }
  throw last;
}

async function run() {
  log("\n[1] /health + non-WS request");
  const health = await fetchRetry(HEALTH);
  ok(health.status === 200, "GET /health -> 200");
  ok((await health.json()).ok === true, "/health body ok=true");
  const root = await fetchRetry(`${HEALTH.replace(/\/health$/, "")}/`);
  ok(root.status === 426, "GET / -> 426");

  log("\n[2] host creates share room");
  const host = await connect();
  send(host, { type: "join-room", roomId: "r1", name: "Host", mode: "share" });
  const hostJoin = await expect(host, "joined-as-host");
  ok(hostJoin.role === "host" && hostJoin.mode === "share", "host got joined-as-host", hostJoin);
  const users0 = await expect(host, "room-users");
  ok(users0.users.length === 1 && users0.users[0].role === "host", "room-users has host only", users0);

  log("\n[3] client requests to join (receive mode)");
  const client = await connect();
  send(client, { type: "join-room", roomId: "r1", name: "Viewer", mode: "receive" });
  const req = await expect(host, "join-request");
  ok(req.name === "Viewer" && req.mode === "receive", "host got join-request", req);
  const pending = await expect(host, "pending-users-update");
  ok(pending.pending.length === 1 && pending.pending[0].id === req.id, "pending-users-update has 1", pending);

  log("\n[4] host re-reads pending");
  send(host, { type: "get-pending-users" });
  const pending2 = await expect(host, "pending-users-update");
  ok(pending2.pending.length === 1, "get-pending-users returns 1", pending2);

  log("\n[5] host approves");
  send(host, { type: "approve-user", targetId: req.id });
  const approved = await expect(client, "approved");
  ok(approved.name === "Viewer" && !!approved.id, "client got approved", approved);
  const pending3 = await expect(host, "pending-users-update");
  ok(pending3.pending.length === 0, "pending cleared", pending3);
  const users1 = await expect(host, "room-users");
  const u1 = await expect(client, "room-users");
  ok(users1.users.length === 2 && u1.users.length === 2, "both see 2 users", [users1, u1]);

  log("\n[6] signal relay client -> host");
  send(client, { type: "signal", payload: { kind: "offer", sdp: "v=0" } });
  const toHost = await expect(host, "signal");
  ok(toHost.fromName === "Viewer" && toHost.payload.sdp === "v=0", "host received signal", toHost);

  log("\n[7] signal relay host -> client");
  send(host, { type: "signal", targetId: approved.id, payload: { kind: "answer" } });
  const toClient = await expect(client, "signal");
  ok(toClient.fromName === "Host" && toClient.payload.kind === "answer", "client received signal", toClient);

  log("\n[8] pending user cannot send signal to host");
  const pendingClient = await connect();
  send(pendingClient, { type: "join-room", roomId: "r1", name: "Pending", mode: "receive" });
  const req2 = await expect(host, "join-request");
  const before = host.queue.filter((m) => m.type === "signal").length;
  send(pendingClient, { type: "signal", payload: { leak: true } });
  const notApproved = await expect(pendingClient, "error");
  ok(
    notApproved.message === "Wait for host approval before signaling",
    "pending user blocked from signaling",
    notApproved
  );
  await new Promise((r) => setTimeout(r, 300));
  ok(host.queue.filter((m) => m.type === "signal").length === before, "host never received pending signal");

  log("\n[9] host rejects pending user");
  send(host, { type: "reject-user", targetId: req2.id });
  const rejected = await expect(pendingClient, "rejected");
  ok(!!rejected.message, "pending user got rejected", rejected);
  await closed(pendingClient);
  ok(true, "rejected socket closed");

  log("\n[10] host leaves -> room-closed broadcast");
  host.close();
  const rc = await expect(client, "room-closed");
  ok(rc.message === "Host has left the room", "client got room-closed", rc);
  await closed(client);
  ok(true, "client socket closed after host left");

  log("\n[11] mode mismatch in full room");
  const h2 = await connect();
  send(h2, { type: "join-room", roomId: "r2", name: "H2", mode: "full" });
  await expect(h2, "joined-as-host");
  const c2 = await connect();
  send(c2, { type: "join-room", roomId: "r2", name: "C2", mode: "share" });
  const mm = await expect(c2, "room-mode-mismatch");
  ok(mm.expectedMode === "full" && mm.roomId === "r2", "share in full room -> expected full", mm);
  await closed(c2);
  h2.close();
  await new Promise((r) => setTimeout(r, 200));

  log("\n[12] share room expects receive");
  const h3 = await connect();
  send(h3, { type: "join-room", roomId: "r3", name: "H3", mode: "share" });
  await expect(h3, "joined-as-host");
  const c3 = await connect();
  send(c3, { type: "join-room", roomId: "r3", name: "C3", mode: "receive" });
  await expect(h3, "join-request");
  ok(true, "receive joins share room");
  const c4 = await connect();
  send(c4, { type: "join-room", roomId: "r3", name: "C4", mode: "full" });
  const mm2 = await expect(c4, "room-mode-mismatch");
  ok(mm2.expectedMode === "receive", "full in share room -> expected receive", mm2);
  await closed(c4);
  h3.close();
  await new Promise((r) => setTimeout(r, 200));

  log("\n[13] receive cannot create a room");
  const c5 = await connect();
  send(c5, { type: "join-room", roomId: "r-nope", name: "C5", mode: "receive" });
  const e1 = await expect(c5, "error");
  ok(e1.message === "Only full or share mode can create a room", "receive cannot create room", e1);
  await closed(c5);

  log("\n[14] invalid mode rejected");
  const c6 = await connect();
  send(c6, { type: "join-room", roomId: "r9", name: "C6", mode: "bogus" });
  const e2 = await expect(c6, "error");
  ok(e2.message === "Invalid room mode", "invalid mode rejected", e2);
  await closed(c6);

  log("\n[15] non-host cannot approve");
  const h4 = await connect();
  send(h4, { type: "join-room", roomId: "r4", name: "H4", mode: "full" });
  await expect(h4, "joined-as-host");
  const c7 = await connect();
  send(c7, { type: "join-room", roomId: "r4", name: "C7", mode: "full" });
  const req4 = await expect(h4, "join-request");
  send(c7, { type: "approve-user", targetId: req4.id });
  await new Promise((r) => setTimeout(r, 300));
  ok(!c7.queue.some((m) => m.type === "approved"), "client self-approve ignored");

  log("\n[16] unknown type + bad json do not kill socket");
  send(c7, { type: "nope" });
  c7.send("{not json");
  const e3 = await expect(c7, "error");
  ok(e3.message === "Invalid JSON payload", "malformed json -> error", e3);
  send(c7, '"just a string"');
  const e4 = await expect(c7, "error");
  ok(e4.message === "Missing message type", "non-object json -> error", e4);
  send(h4, { type: "approve-user", targetId: req4.id });
  const okApproved = await expect(c7, "approved");
  ok(!!okApproved.id, "approve still works after bad messages", okApproved);

  log("\n[17] unjoined socket is swept (alarm)");
  const leaker = await connect();
  send(leaker, { type: "join-room", roomId: "r5", name: "H5", mode: "share" });
  await expect(leaker, "joined-as-host");
  const ghost = await connect();
  const ghostStart = Date.now();
  await closed(ghost, 150000);
  const sweptAfter = Math.round((Date.now() - ghostStart) / 1000);
  ok(sweptAfter <= 90, `ghost closed by alarm after ${sweptAfter}s (<=90s)`);

  h4.close();
  leaker.close();
  await delay(300);
}

main().catch((err) => {
  console.error("\nTEST RUN ERROR:", err.message);
  process.exit(1);
});
