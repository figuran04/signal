const WebSocket = require("ws");
const crypto = require("crypto");

const wss = new WebSocket.Server({
  port: 3001,
});

const rooms = {};

console.log("WebSocket server running on :3001");

wss.on("connection", (ws) => {
  console.log("Client connected");

  ws.id = crypto.randomUUID();

  // ====================================================
  // MESSAGE
  // ====================================================

  ws.on("message", (message) => {
    try {
      const data = JSON.parse(message);

      switch (data.type) {
        // ====================================================
        // JOIN ROOM
        // ====================================================

        case "join-room": {
          const { roomId, name } = data;

          // create room if not exists
          if (!rooms[roomId]) {
            rooms[roomId] = {
              host: ws,
              clients: [],
              pending: [],
            };

            console.log(`Room created: ${roomId}`);
          }

          const room = rooms[roomId];

          ws.roomId = roomId;
          ws.name = name;

          // HOST
          if (room.host === ws) {
            ws.role = "host";

            room.clients.push(ws);

            ws.send(
              JSON.stringify({
                type: "joined-as-host",
                id: ws.id,
                role: ws.role,
              })
            );

            sendRoomUsers(roomId);

            return;
          }

          // CLIENT -> pending approval
          ws.role = "client";

          room.pending.push(ws);

          console.log(`${name} requesting join room ${roomId}`);

          // send request to host
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

        // ====================================================
        // APPROVE USER
        // ====================================================

        case "approve-user": {
          const room = rooms[ws.roomId];

          if (!room) return;

          // only host can approve
          if (room.host !== ws) return;

          const target = room.pending.find(
            (client) => client.id === data.targetId
          );

          if (!target) return;

          // remove from pending
          room.pending = room.pending.filter(
            (client) => client.id !== data.targetId
          );

          // add to clients
          room.clients.push(target);

          console.log(`${target.name} approved`);

          // send approved event
          target.send(
            JSON.stringify({
              type: "approved",
              id: target.id,
              role: target.role,
            })
          );

          sendRoomUsers(ws.roomId);

          return;
        }

        // ====================================================
        // REJECT USER
        // ====================================================

        case "reject-user": {
          const room = rooms[ws.roomId];

          if (!room) return;

          if (room.host !== ws) return;

          const target = room.pending.find(
            (client) => client.id === data.targetId
          );

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

        // ====================================================
        // SIGNALING
        // ====================================================

        case "signal": {
          const room = rooms[ws.roomId];

          if (!room) return;

          let target = null;

          // HOST -> specific client
          if (ws.role === "host") {
            target = room.clients.find(
              (client) => client.id === data.targetId
            );
          }

          // CLIENT -> only HOST
          if (ws.role === "client") {
            target = room.host;
          }

          if (
            target &&
            target.readyState === WebSocket.OPEN
          ) {
            target.send(
              JSON.stringify({
                type: "signal",
                fromId: ws.id,
                payload: data.payload,
              })
            );
          }

          return;
        }

        // ====================================================
        // HOST SEND FILE INFO
        // ====================================================

        case "send-file": {
          const room = rooms[ws.roomId];

          if (!room) return;

          // only host can send to clients
          if (ws.role !== "host") return;

          // broadcast to all clients
          if (data.mode === "broadcast") {
            room.clients.forEach((client) => {
              if (
                client !== ws &&
                client.readyState === WebSocket.OPEN
              ) {
                client.send(
                  JSON.stringify({
                    type: "incoming-file",
                    fromId: ws.id,
                    file: data.file,
                  })
                );
              }
            });

            return;
          }

          // send to selected client
          if (data.targetId) {
            const target = room.clients.find(
              (client) => client.id === data.targetId
            );

            if (
              target &&
              target.readyState === WebSocket.OPEN
            ) {
              target.send(
                JSON.stringify({
                  type: "incoming-file",
                  fromId: ws.id,
                  file: data.file,
                })
              );
            }
          }

          return;
        }

        // ====================================================
        // CLIENT SEND FILE TO HOST
        // ====================================================

        case "client-send-file": {
          const room = rooms[ws.roomId];

          if (!room) return;

          // only client allowed
          if (ws.role !== "client") return;

          const host = room.host;

          if (
            host &&
            host.readyState === WebSocket.OPEN
          ) {
            host.send(
              JSON.stringify({
                type: "incoming-file",
                fromId: ws.id,
                file: data.file,
              })
            );
          }

          return;
        }

        default:
          console.log("Unknown message type:", data.type);
      }
    } catch (err) {
      console.error("Message error:", err.message);
    }
  });

  // ====================================================
  // DISCONNECT
  // ====================================================

  ws.on("close", () => {
    const roomId = ws.roomId;

    if (!roomId) return;

    const room = rooms[roomId];

    if (!room) return;

    // remove from clients
    room.clients = room.clients.filter(
      (client) => client !== ws
    );

    // remove from pending
    room.pending = room.pending.filter(
      (client) => client !== ws
    );

    // host disconnected
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

// ====================================================
// HELPERS
// ====================================================

function sendRoomUsers(roomId) {
  const room = rooms[roomId];

  if (!room) return;

  const users = room.clients.map((client) => ({
    id: client.id,
    name: client.name,
    role: client.role,
  }));

  room.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN) {
      client.send(
        JSON.stringify({
          type: "room-users",
          users,
        })
      );
    }
  });
}