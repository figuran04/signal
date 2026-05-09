const WebSocket = require("ws");

const wss = new WebSocket.Server({
  port: 3001,
});

console.log("WebSocket server running on ws://localhost:3001");

wss.on("connection", (ws) => {
  console.log("Client connected");

  ws.send("Connected to signaling server");

  ws.on("message", (message) => {
    console.log("Received:", message.toString());

    wss.clients.forEach((client) => {
      if (client.readyState === WebSocket.OPEN) {
        client.send(message.toString());
      }
    });
  });

  ws.on("close", () => {
    console.log("Client disconnected");
  });
});