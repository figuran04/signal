const WebSocket = require("ws");
const crypto = require("crypto");

const wss = new WebSocket.Server({
  port: 3001,
});

const rooms = {};

console.log("WebSocket server running on port 3001");

wss.on("connection", (ws) => {
  console.log("Client connected");

  ws.id = crypto.randomUUID();

  ws.on("message", (message) => {
    try {
      const data = JSON.parse(message);

      switch (data.type) {
        case "join-room": {
          const { roomId, name, mode } = data;

          // ✅ validasi mode
          if (!mode || !["full", "share", "receive"].includes(mode)) {
            ws.send(
              JSON.stringify({
                type: "error",
                message: "Invalid room mode",
              })
            );
            ws.close();
            return;
          }

          if (!rooms[roomId]) {
            // ✅ hanya mode 'full' atau 'share' yang bisa membuat room
            if (mode !== "full" && mode !== "share") {
              ws.send(
                JSON.stringify({
                  type: "error",
                  message: "Only full or share mode can create a room",
                })
              );
              ws.close();
              return;
            }

            rooms[roomId] = {
              host: null,
              clients: [],
              pending: [],
              mode: mode,
            };
            console.log(`Room created: ${roomId} with mode: ${mode}`);
          }

          const room = rooms[roomId];

          // ✅ PENgecualian: mode "receive" bisa join ke room APAPUN
          // (share, full, atau receive sendiri)
          const isReceiveMode = mode === "receive";
          const isModeMatch = room.mode === mode;

          // ✅ Jika bukan receive mode, baru cek kesesuaian mode
          if (!isReceiveMode && !isModeMatch) {
            // Tentukan expected mode yang benar untuk redirect
            let expectedMode = room.mode;
            if (room.mode === "share") expectedMode = "receive";
            if (room.mode === "full" && mode === "share") expectedMode = "full";

            ws.send(
              JSON.stringify({
                type: "room-mode-mismatch",
                message: `This room is in ${room.mode} mode. Please use ${expectedMode} mode.`,
                expectedMode: expectedMode,
                roomId: roomId,
              })
            );
            ws.close();
            return;
          }

          ws.roomId = roomId;
          ws.name = name;
          ws.mode = mode;

          // HOST (hanya untuk mode full dan share)
          if (!room.host && (mode === "full" || mode === "share")) {
            ws.role = "host";
            room.host = ws;
            room.clients = [];
            room.pending = [];

            ws.send(
              JSON.stringify({
                type: "joined-as-host",
                id: ws.id,
                name: ws.name,
                role: ws.role,
                mode: room.mode,
              })
            );

            sendRoomUsers(roomId);
            console.log(
              `${name} joined as HOST in room ${roomId} (mode: ${mode})`
            );
            return;
          }

          // ✅ cegah client dengan mode receive menjadi host
          if (!room.host && mode === "receive") {
            ws.send(
              JSON.stringify({
                type: "error",
                message: "No host available in receive-only room",
              })
            );
            ws.close();
            return;
          }

          // CLIENT
          ws.role = "client";

          const alreadyInClients = room.clients.some(
            (client) => client.id === ws.id
          );
          const alreadyInPending = room.pending.some(
            (client) => client.id === ws.id
          );

<<<<<<< Updated upstream
          if (!alreadyInClients && !alreadyInPending) {
            room.pending.push(ws);
            console.log(`${name} requesting to join room ${roomId}`);

            // send request to host
            if (room.host && room.host.readyState === WebSocket.OPEN) {
              room.host.send(
                JSON.stringify({
                  type: "join-request",
                  id: ws.id,
                  name: ws.name,
                })
              );
            } else {
              ws.send(
                JSON.stringify({
                  type: "error",
                  message: "Host is not available",
                })
              );
            }
=======
          // ✅ Jika sudah di clients, kirim approved langsung (reconnect)
          if (alreadyInClients) {
            console.log(
              `${name} already in clients, sending approved directly`
            );
            ws.send(
              JSON.stringify({
                type: "approved",
                id: ws.id,
                name: ws.name,
                role: ws.role,
              })
            );
            return;
>>>>>>> Stashed changes
          }

          // ✅ Jika sudah di pending, skip duplicate
          if (alreadyInPending) {
            console.log(`${name} already in pending, skipping duplicate`);
            return;
          }

          // ✅ Proses request baru
          room.pending.push(ws);
          console.log(
            `${name} requesting to join room ${roomId} (mode: ${mode})`
          );

          if (room.host && room.host.readyState === WebSocket.OPEN) {
            room.host.send(
              JSON.stringify({
                type: "join-request",
                id: ws.id,
                name: ws.name,
                mode: ws.mode,
              })
            );

            const pendingUsersList = room.pending.map((client) => ({
              id: client.id,
              name: client.name,
              mode: client.mode,
            }));

            room.host.send(
              JSON.stringify({
                type: "pending-users-update",
                pending: pendingUsersList,
              })
            );

            console.log(
              `Sent join request to host. Total pending: ${room.pending.length}`
            );
          } else {
            ws.send(
              JSON.stringify({
                type: "error",
                message: "Host is not available",
              })
            );
            room.pending = room.pending.filter((client) => client.id !== ws.id);
          }
          return;
        }

        case "approve-user": {
          const room = rooms[ws.roomId];
          if (!room || room.host !== ws) return;

          const target = room.pending.find(
            (client) => client.id === data.targetId
          );
          if (!target) return;

          room.pending = room.pending.filter(
            (client) => client.id !== data.targetId
          );

          const alreadyExists = room.clients.some(
            (client) => client.id === data.targetId
          );
          if (!alreadyExists) {
            room.clients.push(target);
            console.log(`${target.name} approved`);

<<<<<<< Updated upstream
            target.send(
=======
            if (target.readyState === WebSocket.OPEN) {
              target.send(
                JSON.stringify({
                  type: "approved",
                  id: target.id,
                  name: target.name,
                  role: target.role,
                })
              );
            }
          }

          const pendingUsersList = room.pending.map((client) => ({
            id: client.id,
            name: client.name,
          }));

          if (room.host && room.host.readyState === WebSocket.OPEN) {
            room.host.send(
>>>>>>> Stashed changes
              JSON.stringify({
                type: "approved",
                id: target.id,
                name: target.name,
                role: target.role,
              })
            );
          }

          sendRoomUsers(ws.roomId);
          return;
        }

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

          if (target.readyState === WebSocket.OPEN) {
            target.send(
              JSON.stringify({
                type: "rejected",
                message: "Your join request was rejected",
              })
            );
            target.close(); // Close the connection
          }

<<<<<<< Updated upstream
=======
          const pendingUsersList = room.pending.map((client) => ({
            id: client.id,
            name: client.name,
          }));

          if (ws.readyState === WebSocket.OPEN) {
            ws.send(
              JSON.stringify({
                type: "pending-users-update",
                pending: pendingUsersList,
              })
            );
          }

          console.log(
            `${target.name} rejected. Remaining pending: ${room.pending.length}`
          );
          sendRoomUsers(ws.roomId);
          return;
        }

        case "get-pending-users": {
          const room = rooms[ws.roomId];
          if (!room) return;

          if (room.host !== ws) return;

          const pendingUsersList = room.pending.map((client) => ({
            id: client.id,
            name: client.name,
          }));

          ws.send(
            JSON.stringify({
              type: "pending-users-update",
              pending: pendingUsersList,
            })
          );

>>>>>>> Stashed changes
          return;
        }

        case "signal": {
          const room = rooms[ws.roomId];

          if (!room) return;

          let target = null;

          if (ws.role === "host") {
            target = room.clients.find((client) => client.id === data.targetId);
          }

          if (ws.role === "client") {
            target = room.host;
          }

          if (target && target.readyState === WebSocket.OPEN) {
            target.send(
              JSON.stringify({
                type: "signal",
                fromId: ws.id,
                fromName: ws.name,
                payload: data.payload,
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

  ws.on("close", () => {
    const roomId = ws.roomId;

    if (!roomId) return;

    const room = rooms[roomId];

    if (!room) return;

    console.log(`${ws.name || "Unknown"} disconnected from room ${roomId}`);

    room.clients = room.clients.filter((client) => client !== ws);

    room.pending = room.pending.filter((client) => client !== ws);

    if (room.host === ws) {
      room.clients.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(
            JSON.stringify({
              type: "room-closed",
              message: "Host has left the room",
            })
          );
          client.close();
        }
      });

      room.pending.forEach((client) => {
        if (client.readyState === WebSocket.OPEN) {
          client.send(
            JSON.stringify({
              type: "room-closed",
              message: "Host has left the room",
            })
          );
          client.close();
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

function sendRoomUsers(roomId) {
  const room = rooms[roomId];
  if (!room) return;

  const usersMap = new Map();

  if (room.host && room.host.readyState === WebSocket.OPEN) {
    usersMap.set(room.host.id, {
      id: room.host.id,
      name: room.host.name,
      role: "host",
      connected: true,
    });
  }

  room.clients.forEach((client) => {
    if (client.readyState === WebSocket.OPEN && !usersMap.has(client.id)) {
      usersMap.set(client.id, {
        id: client.id,
        name: client.name,
        role: client.role,
        connected: true,
      });
    }
  });

  const users = Array.from(usersMap.values());

  if (room.host && room.host.readyState === WebSocket.OPEN) {
    room.host.send(
      JSON.stringify({
        type: "room-users",
        users,
      })
    );
  }

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

console.log("WebSocket signaling server is running");
console.log("Waiting for connections...");
