# polly-bridge

KissToy-compatible WebSocket bridge server. Replicates the KnightJenay API protocol exactly and relays commands to local Bluetooth devices via Buttplug.io / Intiface Central.

## Protocol (reverse-engineered)

```
WebSocket:  wss://<host>/websocket-kisstoy?group=<group_hash>
Command:    {"event":"control","data":{"target":"<group>","device_id":"33","motors":{"1":15}}}
Heartbeat:  client → "ping", server → "pong" (every 10s)
```

### Motor Mapping

| Motor | Function | Range | Buttplug Action |
|-------|----------|-------|-----------------|
| 1 | 震动 Vibration (入体端) | 0–15 | `vibrate(intensity)` |
| 3 | 吮吸 Suction | 0–10 | `oscillate(intensity)` or `vibrate(0.7×)` |

## Architecture

```
Browser (KissToy web remote)
    ↓ WSS /websocket-kisstoy?group=...
polly-bridge (Render)
    ↓ WSS /ws/relay?group=...&token=...
Relay Client (PC + Intiface Central)
    ↓ BLE
Device
```

## Deploy on Render

1. Push to GitHub: `git push origin main`
2. Create Web Service on Render, point to repo
3. Set env var `RELAY_SECRET` (or let it auto-generate)
4. Get the URL: `https://polly-bridge.onrender.com`

Or use `render.yaml` for one-click deploy.

## Run Relay Client

On the machine with Bluetooth + Intiface Central:

```bash
pip install -r relay_requirements.txt
python relay_client.py \
  --server wss://polly-bridge.onrender.com \
  --group 82e0ff7c8e3dabe932332e6ea65d272d \
  --token YOUR_RELAY_SECRET
```

## Web Remote Control

Open `https://polly-bridge.onrender.com/remote` — a browser-based remote with two sliders matching the KissToy protocol (Motor 1: 0-15, Motor 3: 0-10).

## API Reference

### REST

| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Server health |
| POST | `/api/session/init` | Create session (returns ws_url) |
| GET | `/api/groups/{group}/devices` | Group status |

### WebSocket (KissToy-compatible)

**Connect:** `wss://host/websocket-kisstoy?group=<hash>`

**Send command:**
```json
{"event":"control","data":{"target":"<group>","device_id":"33","motors":{"1":15,"3":5}}}
```

**Heartbeat:** send `ping` → receive `pong`

**Events received:**
```json
{"event":"device_online","data":{"group":"..."}}
{"event":"device_offline","data":{"group":"..."}}
{"event":"ack","data":{"device_id":"33","motors":{"1":15},"relayed":true}}
```

### WebSocket (Relay)

**Connect:** `wss://host/ws/relay?group=<hash>&token=<secret>`

**Receives:** `{"type":"command","group":"...","device_id":"33","motors":{"1":15}}`

**Sends:** `{"type":"status","online":true,"motors":{"1":15}}`

## Safety

- Dead man's switch: relay disconnect → all motors stop
- Heartbeat monitor every 10s
- Rate limiting: 120 commands/min per session
- HMAC-signed commands between server and relay
