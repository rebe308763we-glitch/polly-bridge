"""
polly-bridge — KissToy Protocol Compatible Bridge Server
=========================================================
Replicates the KnightJenay/KissToy WebSocket API exactly, then relays
commands to a local machine running Intiface Central via Buttplug.io.

Protocol (reverse-engineered from api.app.knightjenay.cn):
  WebSocket: wss://<host>/websocket-kisstoy?group=<group_hash>
  Command:   {"event":"control","data":{"target":"<group>","device_id":"33","motors":{"1":15}}}
  Heartbeat: client sends "ping", server replies "pong" (every 10s)
  Motors:    1 = vibrate (0-15), 3 = suction (0-10)

Architecture:
  Browser (KissToy-compatible)
      ↓ WSS /websocket-kisstoy?group=...
  polly-bridge (Render)
      ↓ WSS /ws/relay?group=...&token=...
  Relay Client (local PC + Intiface Central)
      ↓ BLE
  Device
"""

import asyncio
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from datetime import datetime, timezone
from typing import Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
import uvicorn

# ── Config ──────────────────────────────────────────────────────────
RELAY_SECRET = os.environ.get("RELAY_SECRET", secrets.token_hex(32))
PORT = int(os.environ.get("PORT", "8000"))

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polly-bridge")

# ── App ─────────────────────────────────────────────────────────────
app = FastAPI(title="polly-bridge", version="0.2.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── State ───────────────────────────────────────────────────────────
# group_id -> set of browser WebSockets
group_controllers: dict[str, set[WebSocket]] = {}
# group_id -> set of relay WebSockets
group_relays: dict[str, set[WebSocket]] = {}
# session_id -> metadata
sessions: dict[str, dict] = {}
# Rate limiting
rate_buckets: dict[str, list[float]] = {}
RATE_MAX = 120  # commands per window
RATE_WINDOW = 60  # seconds


def check_rate(key: str) -> bool:
    now = time.time()
    bucket = rate_buckets.setdefault(key, [])
    bucket[:] = [t for t in bucket if now - t < RATE_WINDOW]
    if len(bucket) >= RATE_MAX:
        return False
    bucket.append(now)
    return True


def sign(payload: dict) -> str:
    raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hmac.new(RELAY_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()


# ── Models ──────────────────────────────────────────────────────────
class SessionInit(BaseModel):
    device_id: str
    group: Optional[str] = None
    user_id: Optional[str] = None
    lang: str = "zh"


class DeviceCommand(BaseModel):
    device_id: str
    group: str
    motors: dict[str, int] = Field(default_factory=dict)
    action: Optional[str] = None
    intensity: Optional[float] = None


# ── REST API ────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "service": "polly-bridge",
        "version": "0.2.0",
        "protocol": "KissToy-compatible",
        "ws_endpoint": "/websocket-kisstoy?group=<group_hash>",
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "groups": len(group_controllers),
        "relays": sum(len(r) for r in group_relays.values()),
        "controllers": sum(len(c) for c in group_controllers.values()),
    }


@app.post("/api/session/init")
async def init_session(session: SessionInit, request: Request):
    """Initialize a control session — returns WebSocket URL in KissToy format."""
    group = session.group or secrets.token_hex(16)
    host = request.headers.get("host", "localhost")

    return {
        "ws_url": f"wss://{host}/websocket-kisstoy?group={group}",
        "group": group,
        "device_id": session.device_id,
        "motors": {
            "1": {"name": "震动 (Vibrate)", "range": [0, 15]},
            "3": {"name": "吮吸 (Suction)", "range": [0, 10]},
        },
    }


@app.get("/api/groups/{group}/devices")
async def group_devices(group: str):
    """List devices in a group."""
    relay_count = len(group_relays.get(group, set()))
    controller_count = len(group_controllers.get(group, set()))
    return {
        "group": group,
        "relays_connected": relay_count,
        "controllers_connected": controller_count,
        "online": relay_count > 0,
    }


# ── KissToy-Compatible WebSocket ────────────────────────────────────
@app.websocket("/websocket-kisstoy")
async def ws_kisstoy(websocket: WebSocket, group: str = Query(...)):
    """
    Main control WebSocket — compatible with KissToy/KnightJenay protocol.

    Browser connects:  wss://host/websocket-kisstoy?group=<group_hash>
    Sends:             {"event":"control","data":{"target":"<group>","device_id":"33","motors":{"1":15}}}
    Heartbeat:         client sends "ping" → server replies "pong"
    """
    await websocket.accept()
    group_controllers.setdefault(group, set()).add(websocket)
    client_ip = websocket.client.host if websocket.client else "unknown"
    log.info(f"[KissToy] Browser connected: group={group[:12]}... from {client_ip}")

    # Notify relays that a controller joined
    for relay in group_relays.get(group, set()):
        try:
            await relay.send_json({
                "type": "controller_online",
                "group": group,
                "timestamp": time.time(),
            })
        except Exception:
            pass

    last_ping = time.time()
    try:
        while True:
            raw = await websocket.receive_text()

            # Heartbeat: ping/pong
            if raw.strip() == "ping":
                await websocket.send_text("pong")
                last_ping = time.time()
                continue

            # Parse JSON command
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({"event": "error", "data": {"message": "Invalid JSON"}}))
                continue

            event = msg.get("event", "")
            data = msg.get("data", {})

            if event == "control":
                target = data.get("target", group)
                device_id = data.get("device_id", "")
                motors = data.get("motors", {})

                # Rate limit
                rate_key = f"{group}:{client_ip}"
                if not check_rate(rate_key):
                    await websocket.send_text(json.dumps({
                        "event": "error",
                        "data": {"message": "Rate limit exceeded"},
                    }))
                    continue

                log.info(f"[KissToy] Control: group={group[:12]}... device={device_id} motors={motors}")

                # Forward to relays in this group
                payload = {
                    "type": "command",
                    "group": target,
                    "device_id": device_id,
                    "motors": motors,
                    "timestamp": time.time(),
                    "signature": "",
                }
                payload["signature"] = sign(payload)

                forwarded = False
                for relay in list(group_relays.get(target, set())):
                    try:
                        await relay.send_json(payload)
                        forwarded = True
                    except Exception:
                        group_relays[target].discard(relay)

                # Ack to browser
                await websocket.send_text(json.dumps({
                    "event": "ack",
                    "data": {
                        "device_id": device_id,
                        "motors": motors,
                        "relayed": forwarded,
                    },
                }))

            elif event == "ping":
                await websocket.send_text(json.dumps({"event": "pong"}))

            else:
                log.debug(f"[KissToy] Unknown event: {event}")

    except WebSocketDisconnect:
        log.info(f"[KissToy] Browser disconnected: group={group[:12]}...")
        group_controllers.get(group, set()).discard(websocket)
        if not group_controllers.get(group):
            group_controllers.pop(group, None)

        # Notify relays
        for relay in group_relays.get(group, set()):
            try:
                await relay.send_json({
                    "type": "controller_offline",
                    "group": group,
                    "timestamp": time.time(),
                })
            except Exception:
                pass
    except Exception as e:
        log.error(f"[KissToy] WS error: {e}")
        group_controllers.get(group, set()).discard(websocket)


# ── Relay WebSocket (for local Intiface client) ─────────────────────
@app.websocket("/ws/relay")
async def ws_relay(websocket: WebSocket, group: str = Query(...), token: str = Query(...)):
    """
    Relay client connection. Talks Buttplug.io to the actual device.

    Local machine connects:  wss://host/ws/relay?group=<hash>&token=<secret>
    Receives:                {"type":"command","group":"...","motors":{"1":15}}
    Sends back:              {"type":"status","motors":{...},"battery":...}
    """
    if token != RELAY_SECRET:
        await websocket.close(code=4001, reason="Invalid token")
        return

    await websocket.accept()
    group_relays.setdefault(group, set()).add(websocket)
    log.info(f"[Relay] Connected: group={group[:12]}...")

    # Notify browser controllers
    for ctrl in group_controllers.get(group, set()):
        try:
            await ctrl.send_text(json.dumps({
                "event": "device_online",
                "data": {"group": group},
            }))
        except Exception:
            pass

    missed_pings = 0
    try:
        while True:
            try:
                raw = await asyncio.wait_for(websocket.receive_text(), timeout=15)
            except asyncio.TimeoutError:
                missed_pings += 1
                if missed_pings >= 3:
                    log.warning(f"[Relay] Group {group[:12]}... heartbeat timeout")
                    break
                try:
                    await websocket.send_json({"type": "ping", "timestamp": time.time()})
                except Exception:
                    break
                continue

            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            msg_type = msg.get("type", "")

            if msg_type == "pong":
                missed_pings = 0

            elif msg_type == "status":
                # Forward status to browser controllers
                for ctrl in group_controllers.get(group, set()):
                    try:
                        await ctrl.send_text(json.dumps({
                            "event": "status",
                            "data": msg,
                        }))
                    except Exception:
                        group_controllers.get(group, set()).discard(ctrl)

            elif msg_type == "device_info":
                # Device capabilities broadcast
                for ctrl in group_controllers.get(group, set()):
                    try:
                        await ctrl.send_text(json.dumps({
                            "event": "device_info",
                            "data": msg,
                        }))
                    except Exception:
                        pass

    except WebSocketDisconnect:
        log.info(f"[Relay] Disconnected: group={group[:12]}...")
    except Exception as e:
        log.error(f"[Relay] Error: {e}")
    finally:
        group_relays.get(group, set()).discard(websocket)
        if not group_relays.get(group):
            group_relays.pop(group, None)

        # Notify browsers
        for ctrl in group_controllers.get(group, set()):
            try:
                await ctrl.send_text(json.dumps({
                    "event": "device_offline",
                    "data": {"group": group},
                }))
            except Exception:
                pass


# ── Heartbeat Monitor ───────────────────────────────────────────────
@app.on_event("startup")
async def start_monitor():
    asyncio.create_task(heartbeat_monitor())


async def heartbeat_monitor():
    """Clean up stale relay connections."""
    while True:
        await asyncio.sleep(10)
        for group, relays in list(group_relays.items()):
            for relay in list(relays):
                try:
                    await relay.send_json({"type": "ping", "timestamp": time.time()})
                except Exception:
                    relays.discard(relay)
                    log.info(f"[Monitor] Removed stale relay for group={group[:12]}...")
            if not relays:
                group_relays.pop(group, None)


# ── Static Files ────────────────────────────────────────────────────
import os as _os
_static_dir = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "static")
if _os.path.isdir(_static_dir):
    app.mount("/remote", StaticFiles(directory=_static_dir, html=True), name="static")


@app.get("/remote")
async def remote_control():
    index_path = _os.path.join(_static_dir, "index.html")
    if _os.path.isfile(index_path):
        return FileResponse(index_path)
    return HTMLResponse("<h1>static/index.html not found</h1>", status_code=404)


# ── Entrypoint ──────────────────────────────────────────────────────
if __name__ == "__main__":
    log.info(f"Starting polly-bridge v0.2.0 on port {PORT}")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
