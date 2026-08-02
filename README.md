# polly-bridge

KissToy WebSocket relay. Forwards control commands from browsers/Claude to the real KissToy/KnightJenay WebSocket server.

## Architecture

```
Browser / Claude (MCP)
    ↓ WSS /websocket-kisstoy?group=...
polly-bridge (Render)
    ↓ WSS /websocket-kisstoy?group=...
api.app.knightjenay.cn (KissToy server)
    ↓
Phone App
    ↓ BLE
Polly device
```

polly-bridge is a **transparent relay** — it does not talk BLE directly. It just forwards WebSocket messages between you and the real KissToy server.

## Protocol (KissToy-compatible)

| Direction | Format |
|-----------|--------|
| Connect | `wss://host/websocket-kisstoy?group=<hash>` |
| Command | `{"event":"control","data":{"target":"<group>","device_id":"33","motors":{"1":15}}}` |
| Heartbeat | `"ping"` → `"pong"` (every 10s) |

### Motor Mapping

| Motor | Function | Range |
|-------|----------|-------|
| 1 | 震动 Vibrate (入体端) | 0–15 |
| 3 | 吮吸 Suction | 0–10 |

## Deploy on Render

Push to GitHub → Render auto-deploys from `render.yaml`.

Set env var `KISSTOY_WS` if the upstream URL changes (default: `wss://api.app.knightjenay.cn/websocket-kisstoy`).

## API

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Server health + connected clients |
| POST | `/api/session/init` | Create session (returns ws_url) |
| WSS | `/websocket-kisstoy?group=...` | Main control WebSocket relay |
| GET | `/remote` | Browser remote control UI |
