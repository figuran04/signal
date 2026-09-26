export const ROOM_MODES = ["full", "share", "receive"];

export const HOST_MODES = ["full", "share"];

export function send(ws, payload) {
  try {
    if (ws.readyState !== 1) return;
    ws.send(JSON.stringify(payload));
  } catch (err) {
    console.error("send failed", err);
  }
}

export function close(ws, code = 1000) {
  try {
    ws.close(code);
  } catch (err) {
    console.error("close failed", err);
  }
}

export function expectedModeFor(roomMode, joinMode) {
  if (roomMode === "share") return "receive";
  if (roomMode === "full" && joinMode === "share") return "full";
  return roomMode;
}
