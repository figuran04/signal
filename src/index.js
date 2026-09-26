import { SignalRoom } from "./room.js";

export { SignalRoom };

const ROUTER_NAME = "signal-router";

const CORS = { "access-control-allow-origin": "*" };

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return new Response(
        JSON.stringify({ ok: true, service: "signal", time: new Date().toISOString() }),
        { headers: { "content-type": "application/json; charset=utf-8", ...CORS } }
      );
    }

    if (request.headers.get("Upgrade") !== "websocket") {
      return new Response(
        "signal signaling server. Connect over WebSocket: wss://<this-host>/\n",
        { status: 426, headers: { "content-type": "text/plain; charset=utf-8", ...CORS } }
      );
    }

    const id = env.SIGNAL_ROOM.idFromName(ROUTER_NAME);
    return env.SIGNAL_ROOM.get(id).fetch(request);
  },
};
