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

          if (!rooms[roomId]) {
            rooms[roomId] = {
              host: null,
              clients: [],
              pending: [],
            };
            console.log(`Room created: ${roomId}`);
          }

          const room = rooms[roomId];
          ws.roomId = roomId;
          ws.name = name;

          // HOST (room has no host yet)
          if (!room.host) {
            ws.role = "host";
            room.host = ws;

            // Clear existing clients and pending
            room.clients = [];
            room.pending = [];

            ws.send(
              JSON.stringify({
                type: "joined-as-host",
                id: ws.id,
                name: ws.name,
                role: ws.role,
              })
            );

            sendRoomUsers(roomId);
            console.log(`${name} joined as HOST in room ${roomId}`);
            return;
          }

          // CLIENT -> pending approval
          ws.role = "client";

          // Cek apakah sudah ada di clients atau pending
          const alreadyInClients = room.clients.some(
            (client) => client.id === ws.id
          );
          const alreadyInPending = room.pending.some(
            (client) => client.id === ws.id
          );

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
          }

          return;
        }

        // ====================================================
        // APPROVE USER
        // ====================================================

        // Di backend, pastikan saat approve user tidak double add
        case "approve-user": {
          const room = rooms[ws.roomId];
          if (!room || room.host !== ws) return;

          const target = room.pending.find(
            (client) => client.id === data.targetId
          );
          if (!target) return;

          // Remove from pending
          room.pending = room.pending.filter(
            (client) => client.id !== data.targetId
          );

          // Check if already in clients
          const alreadyExists = room.clients.some(
            (client) => client.id === data.targetId
          );
          if (!alreadyExists) {
            room.clients.push(target);
            console.log(`${target.name} approved`);

            target.send(
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

          if (target.readyState === WebSocket.OPEN) {
            target.send(
              JSON.stringify({
                type: "rejected",
                message: "Your join request was rejected",
              })
            );
            target.close(); // Close the connection
          }

          return;
        }

        // ====================================================
        // SIGNALING (WebRTC)
        // ====================================================

        case "signal": {
          const room = rooms[ws.roomId];

          if (!room) return;

          let target = null;

          // HOST -> specific client
          if (ws.role === "host") {
            target = room.clients.find((client) => client.id === data.targetId);
          }

          // CLIENT -> only HOST
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

  // ====================================================
  // DISCONNECT
  // ====================================================

  ws.on("close", () => {
    const roomId = ws.roomId;

    if (!roomId) return;

    const room = rooms[roomId];

    if (!room) return;

    console.log(`${ws.name || "Unknown"} disconnected from room ${roomId}`);

    // remove from clients
    room.clients = room.clients.filter((client) => client !== ws);

    // remove from pending
    room.pending = room.pending.filter((client) => client !== ws);

    // host disconnected
    if (room.host === ws) {
      // Notify all clients that room is closed
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

    // Update remaining users
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

  // Use Map to ensure unique users
  const usersMap = new Map();

  // Add host
  if (room.host && room.host.readyState === WebSocket.OPEN) {
    usersMap.set(room.host.id, {
      id: room.host.id,
      name: room.host.name,
      role: "host",
      connected: true,
    });
  }

  // Add clients
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

  // Send to host
  if (room.host && room.host.readyState === WebSocket.OPEN) {
    room.host.send(
      JSON.stringify({
        type: "room-users",
        users,
      })
    );
  }

  // Send to all clients
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

// Optional: Health check endpoint for monitoring
console.log("WebSocket signaling server is running");
console.log("Waiting for connections...");
