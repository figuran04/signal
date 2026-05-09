const WebSocket = require("ws");

const wss = new WebSocket.Server({
  port: 3001,
});

const rooms = {};

console.log("WebSocket server running on ws://localhost:3001");

wss.on("connection", (ws) => {
  console.log("Client connected");

  ws.id = crypto.randomUUID();

  ws.on("message", (message) => {
    const data = JSON.parse(message);

    // =========================
    // JOIN ROOM
    // =========================
    if (data.type === "join-room") {
      const { roomId, name } = data;

      // buat room jika belum ada
      if (!rooms[roomId]) {
        rooms[roomId] = {
          host: ws,
          locked: false,
          clients: [],
          pending: [],
        };

        console.log(`Room created: ${roomId}`);
      }

      const room = rooms[roomId];

      // room terkunci
      if (room.locked) {
        ws.send(
          JSON.stringify({
            type: "room-locked",
          })
        );

        return;
      }

      ws.roomId = roomId;
      ws.name = name;

      // host otomatis approved
      if (room.host === ws) {
        room.clients.push(ws);

        ws.send(
          JSON.stringify({
            type: "joined-as-host",
            id: ws.id,
          })
        );

        sendRoomUsers(roomId);

        return;
      }

      // user masuk pending
      room.pending.push(ws);

      console.log(`${name} requesting join room ${roomId}`);

      // kirim request ke host
      if (room.host.readyState === WebSocket.OPEN) {
        room.host.send(
          JSON.stringify({
            type: "join-request",
            id: ws.id,
            name: ws.name,
          })
        );
      }

      return;
    }

    // =========================
    // APPROVE USER
    // =========================
    if (data.type === "approve-user") {
      const room = rooms[ws.roomId];

      if (!room) return;

      // hanya host boleh approve
      if (room.host !== ws) return;

      const target = room.pending.find((client) => client.id === data.targetId);

      if (!target) return;

      // pindahkan pending -> clients
      room.pending = room.pending.filter(
        (client) => client.id !== data.targetId
      );

      room.clients.push(target);

      // lock room
      room.locked = true;

      console.log(`${target.name} approved`);

      // kirim approval ke target
      target.send(
        JSON.stringify({
          type: "approved",
        })
      );

      // update semua user
      sendRoomUsers(ws.roomId);

      return;
    }

    // =========================
    // REJECT USER
    // =========================
    if (data.type === "reject-user") {
      const room = rooms[ws.roomId];

      if (!room) return;

      if (room.host !== ws) return;

      const target = room.pending.find((client) => client.id === data.targetId);

      if (!target) return;

      room.pending = room.pending.filter(
        (client) => client.id !== data.targetId
      );

      target.send(
        JSON.stringify({
          type: "rejected",
        })
      );

      return;
    }

    // =========================
    // WEBRTC SIGNALING
    // =========================

    const room = rooms[ws.roomId];

    if (!room) return;

    room.clients.forEach((client) => {
      if (client !== ws && client.readyState === WebSocket.OPEN) {
        client.send(message.toString());
      }
    });
  });

  ws.on("close", () => {
    const roomId = ws.roomId;

    if (!roomId) return;

    const room = rooms[roomId];

    if (!room) return;

    room.clients = room.clients.filter((client) => client !== ws);

    room.pending = room.pending.filter((client) => client !== ws);

    // host keluar
    if (room.host === ws) {
      room.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(
            JSON.stringify({
              type: "room-closed",
            })
          );
        }
      });

      delete rooms[roomId];

      console.log(`Room deleted: ${roomId}`);

      return;
    }

    sendRoomUsers(roomId);

    console.log("Client disconnected");
  });
});

// =========================
// HELPER
// =========================

function sendRoomUsers(roomId) {
  const room = rooms[roomId];

  if (!room) return;

  const users = room.clients.map((client) => ({
    id: client.id,
    name: client.name,
  }));

  room.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(
        JSON.stringify({
          type: "room-users",
          users,
          locked: room.locked,
        })
      );
    }
  });
}
