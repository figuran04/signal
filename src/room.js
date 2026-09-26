import { close, expectedModeFor, HOST_MODES, ROOM_MODES, send } from "./messages.js";

const UNJOINED_MAX_AGE_MS = 60_000;
const SWEEP_INTERVAL_MS = 5 * 60_000;

export class SignalRoom {
  constructor(ctx, env) {
    this.ctx = ctx;
    this.env = env;
  }

  async fetch(request) {
    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response("Expected a WebSocket upgrade request", { status: 426 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];

    this.ctx.acceptWebSocket(server);
    server.serializeAttachment({ state: "unjoined", since: Date.now() });

    await this.scheduleSweep();

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws, message) {
    const raw = typeof message === "string" ? message : new TextDecoder().decode(message);

    let data;
    try {
      data = JSON.parse(raw);
    } catch {
      send(ws, { type: "error", message: "Invalid JSON payload" });
      return;
    }

    if (!data || typeof data.type !== "string") {
      send(ws, { type: "error", message: "Missing message type" });
      return;
    }

    try {
      switch (data.type) {
        case "join-room":
          this.handleJoin(ws, data);
          break;
        case "approve-user":
          this.handleApprove(ws, data);
          break;
        case "reject-user":
          this.handleReject(ws, data);
          break;
        case "get-pending-users":
          this.handleGetPending(ws);
          break;
        case "signal":
          this.handleSignal(ws, data);
          break;
        default:
          console.log("Unknown message type:", data.type);
      }
    } catch (err) {
      console.error("handler failed for", data.type, err);
      send(ws, { type: "error", message: "Internal server error" });
    }
  }

  async webSocketClose(ws) {
    await this.handleLeave(ws);
  }

  async webSocketError(ws) {
    await this.handleLeave(ws);
  }

  handleJoin(ws, data) {
    const { roomId, name, mode } = data;

    if (typeof roomId !== "string" || roomId.length === 0 || roomId.length > 128) {
      send(ws, { type: "error", message: "Invalid room id" });
      close(ws);
      return;
    }

    if (!ROOM_MODES.includes(mode)) {
      send(ws, { type: "error", message: "Invalid room mode" });
      close(ws);
      return;
    }

    const { host, mode: roomMode } = this.room(roomId);

    if (!roomMode) {
      if (!HOST_MODES.includes(mode)) {
        send(ws, {
          type: "error",
          message: "Only full or share mode can create a room",
        });
        close(ws);
        return;
      }

      const hostMeta = {
        id: crypto.randomUUID(),
        name: typeof name === "string" ? name : "Guest",
        mode,
        role: "host",
        roomId,
        pending: false,
      };
      this.attach(ws, hostMeta);

      console.log(`room created: ${roomId} (mode: ${mode})`);
      send(ws, {
        type: "joined-as-host",
        id: hostMeta.id,
        name: hostMeta.name,
        role: "host",
        mode,
      });

      this.notifyUsers(roomId);
      return;
    }

    if (mode !== "receive" && mode !== roomMode) {
      send(ws, {
        type: "room-mode-mismatch",
        message: `This room is in ${roomMode} mode. Please use ${expectedModeFor(
          roomMode,
          mode
        )} mode.`,
        expectedMode: expectedModeFor(roomMode, mode),
        roomId,
      });
      close(ws);
      return;
    }

    if (!host) {
      send(ws, {
        type: "error",
        message: "No host available in receive-only room",
      });
      close(ws);
      return;
    }

    if (host.ws.readyState !== 1) {
      send(ws, { type: "error", message: "Host is not available" });
      return;
    }

    const id = crypto.randomUUID();
    this.attach(ws, {
      id,
      name: typeof name === "string" ? name : "Guest",
      mode,
      role: "client",
      roomId,
      pending: true,
    });

    send(host.ws, { type: "join-request", id, name, mode });
    this.notifyPending(roomId);
  }

  handleApprove(ws, data) {
    const actor = this.meta(ws);
    if (!actor || !this.isHost(ws, actor.roomId)) return;

    const target = this.room(actor.roomId).members.find(
      (m) => m.meta.id === data.targetId && m.meta.pending
    );
    if (!target) return;

    target.ws.serializeAttachment({ ...target.meta, pending: false });
    console.log(`approved: ${target.meta.name} in room ${actor.roomId}`);

    send(target.ws, {
      type: "approved",
      id: target.meta.id,
      name: target.meta.name,
      role: target.meta.role,
    });

    this.notifyPending(actor.roomId);
    this.notifyUsers(actor.roomId);
  }

  handleReject(ws, data) {
    const actor = this.meta(ws);
    if (!actor || !this.isHost(ws, actor.roomId)) return;

    const target = this.room(actor.roomId).members.find(
      (m) => m.meta.id === data.targetId && m.meta.pending
    );
    if (!target) return;

    send(target.ws, {
      type: "rejected",
      message: "Your join request was rejected by the host",
      roomId: actor.roomId,
    });
    close(target.ws);

    this.notifyPending(actor.roomId);
    this.notifyUsers(actor.roomId);
  }

  handleGetPending(ws) {
    const actor = this.meta(ws);
    if (!actor || !this.isHost(ws, actor.roomId)) return;
    this.notifyPending(actor.roomId);
  }

  handleSignal(ws, data) {
    const actor = this.meta(ws);
    if (!actor) return;

    if (actor.pending) {
      send(ws, { type: "error", message: "Wait for host approval before signaling" });
      return;
    }

    const { members, host } = this.room(actor.roomId);
    if (!host) return;

    let target = null;
    if (actor.role === "host") {
      target = members.find((m) => m.meta.id === data.targetId && !m.meta.pending);
    } else if (actor.role === "client") {
      target = host;
    }
    if (!target) return;

    send(target.ws, {
      type: "signal",
      fromId: actor.id,
      fromName: actor.name,
      payload: data.payload,
    });
  }

  async handleLeave(ws) {
    const meta = this.meta(ws);
    if (!meta) return;

    if (meta.role === "host") {
      const notice = { type: "room-closed", message: "Host has left the room" };
      for (const member of this.room(meta.roomId).members) {
        if (member.ws === ws) continue;
        send(member.ws, notice);
        close(member.ws);
      }
      console.log(`room closed: ${meta.roomId}`);
      return;
    }

    console.log(`${meta.name} left room ${meta.roomId}`);
    this.notifyUsers(meta.roomId);
  }

  meta(ws) {
    const attachment = ws.deserializeAttachment();
    return attachment && attachment.state === "joined" ? attachment : null;
  }

  attach(ws, meta) {
    ws.serializeAttachment({ state: "joined", ...meta });
  }

  members(roomId) {
    const list = [];
    for (const ws of this.ctx.getWebSockets()) {
      const meta = this.meta(ws);
      if (!meta) continue;
      if (roomId && meta.roomId !== roomId) continue;
      list.push({ ws, meta });
    }
    return list;
  }

  room(roomId) {
    const members = this.members(roomId);
    const host = members.find((m) => m.meta.role === "host") ?? null;
    return { members, host, mode: host ? host.meta.mode : null };
  }

  isHost(ws, roomId) {
    return this.room(roomId).host?.ws === ws;
  }

  notifyPending(roomId) {
    const host = this.room(roomId).host;
    if (!host) return;

    const pending = this.members(roomId)
      .filter((m) => m.meta.pending)
      .map((m) => ({ id: m.meta.id, name: m.meta.name, mode: m.meta.mode }));

    send(host.ws, { type: "pending-users-update", pending });
  }

  notifyUsers(roomId) {
    const { members, host } = this.room(roomId);
    if (!host) return;

    const users = members.map((m) => ({
      id: m.meta.id,
      name: m.meta.name,
      role: m.meta.role,
      connected: true,
    }));

    for (const member of members) {
      send(member.ws, { type: "room-users", users });
    }
  }

  async scheduleSweep() {
    const now = Date.now();
    const existing = await this.ctx.storage.getAlarm();

    // Always arm an alarm that fires no later than the soonest grace deadline,
    // otherwise a socket that never sends join-room could sit for 2x the interval.
    let due = now + SWEEP_INTERVAL_MS;
    for (const ws of this.ctx.getWebSockets()) {
      const attachment = ws.deserializeAttachment();
      if (!attachment || attachment.state !== "unjoined") continue;
      const deadline = (attachment.since ?? now) + UNJOINED_MAX_AGE_MS;
      if (deadline < due) due = deadline;
    }

    if (existing === null || existing > due) {
      await this.ctx.storage.setAlarm(due);
    }
  }

  async alarm() {
    const now = Date.now();
    let waiting = 0;

    for (const ws of this.ctx.getWebSockets()) {
      const attachment = ws.deserializeAttachment();
      if (!attachment || attachment.state !== "unjoined") continue;

      if (now - (attachment.since ?? 0) >= UNJOINED_MAX_AGE_MS) {
        send(ws, { type: "error", message: "No join-room message received in time" });
        close(ws, 1008);
      } else {
        waiting += 1;
      }
    }

    if (waiting > 0) {
      let due = now + SWEEP_INTERVAL_MS;
      for (const ws of this.ctx.getWebSockets()) {
        const attachment = ws.deserializeAttachment();
        if (!attachment || attachment.state !== "unjoined") continue;
        const deadline = (attachment.since ?? now) + UNJOINED_MAX_AGE_MS;
        if (deadline < due) due = deadline;
      }
      await this.ctx.storage.setAlarm(due);
    } else {
      await this.ctx.storage.deleteAlarm();
    }
  }
}
