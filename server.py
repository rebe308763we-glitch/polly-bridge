"""
polly-bridge — WebSocket Pairing Relay
=======================================
Pairs browser controllers with a local relay client, forwarding commands
bidirectionally. The local relay handles the actual KissToy connection.

Chain:
  Browser → polly-bridge (Render) → local_relay.py (your PC) → KissToy WS → phone → BLE → device
"""

import asyncio
import json
import logging
import os
import time

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn

# ── Config ──────────────────────────────────────────────────────────
PORT = int(os.environ.get("PORT", "8000"))
RATE_MAX = 120
RATE_WINDOW = 60

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polly-bridge")

# ── App ─────────────────────────────────────────────────────────────
app = FastAPI(title="polly-bridge", version="0.4.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── State ───────────────────────────────────────────────────────────
# group_id -> relay WebSocket (local_relay.py)
group_relays: dict[str, WebSocket] = {}
# group_id -> set of browser WebSockets
group_clients: dict[str, set[WebSocket]] = {}
rate_buckets: dict[str, list[float]] = {}


def check_rate(key: str) -> bool:
    now = time.time()
    bucket = rate_buckets.setdefault(key, [])
    bucket[:] = [t for t in bucket if now - t < RATE_WINDOW]
    if len(bucket) >= RATE_MAX:
        return False
    bucket.append(now)
    return True


# ── Models ──────────────────────────────────────────────────────────
class SessionInit(BaseModel):
    device_id: str
    group: str = ""
    session_id: str = ""
    lang: str = "zh"


# ── REST ────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "service": "polly-bridge",
        "version": "0.4.0",
        "mode": "WebSocket pairing relay",
        "groups": len(group_relays),
        "clients": sum(len(c) for c in group_clients.values()),
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "groups": len(group_relays),
        "clients": sum(len(c) for c in group_clients.values()),
        "relays": len(group_relays),
    }


@app.post("/api/session/init")
async def init_session(session: SessionInit, request: Request):
    host = request.headers.get("host", "localhost")
    return {
        "ws_url": f"wss://{host}/websocket-kisstoy?group={session.group}&id={session.session_id}",
        "group": session.group,
        "device_id": session.device_id,
    }


# ── Browser WebSocket ───────────────────────────────────────────────
@app.websocket("/websocket-kisstoy")
async def ws_browser(websocket: WebSocket, group: str = Query(...), id: str = Query(default="")):
    """Browser connects here. Commands forwarded to relay, responses from relay sent back."""
    await websocket.accept()
    group_clients.setdefault(group, set()).add(websocket)
    client_ip = websocket.client.host if websocket.client else "?"
    log.info(f"[Browser] Connected: group={group[:12]}... id={id} from {client_ip}")

    try:
        while True:
            raw = await websocket.receive_text()

            # Local heartbeat
            if raw.strip() == "ping":
                await websocket.send_text("pong")
                continue

            # Parse
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            event = msg.get("event", "")

            if event == "control":
                if not check_rate(f"{group}:{client_ip}"):
                    await websocket.send_text(json.dumps({
                        "event": "error", "data": {"message": "Rate limit exceeded"},
                    }))
                    continue

                motors = msg.get("data", {}).get("motors", {})
                log.info(f"[Browser →] group={group[:12]}... motors={motors}")

                # Forward to relay
                relay = group_relays.get(group)
                if relay:
                    try:
                        await relay.send_text(raw)
                        await websocket.send_text(json.dumps({
                            "event": "ack",
                            "data": {"motors": motors, "relayed": True},
                        }))
                    except Exception:
                        group_relays.pop(group, None)
                        await websocket.send_text(json.dumps({
                            "event": "ack",
                            "data": {"motors": motors, "relayed": False, "message": "relay disconnected"},
                        }))
                else:
                    await websocket.send_text(json.dumps({
                        "event": "ack",
                        "data": {"motors": motors, "relayed": False, "message": "no relay connected"},
                    }))

    except WebSocketDisconnect:
        log.info(f"[Browser] Disconnected: group={group[:12]}...")
    finally:
        group_clients.get(group, set()).discard(websocket)
        if not group_clients.get(group):
            group_clients.pop(group, None)


# ── Relay WebSocket (local_relay.py) ────────────────────────────────
@app.websocket("/ws/relay")
async def ws_relay(websocket: WebSocket, group: str = Query(...)):
    """local_relay.py connects here. Forwards browser commands to relay, relay responses to browsers."""
    await websocket.accept()
    group_relays[group] = websocket
    log.info(f"[Relay] Connected: group={group[:12]}...")

    # Notify browsers
    for client in group_clients.get(group, set()):
        try:
            await client.send_text(json.dumps({
                "event": "relay_online",
                "data": {"group": group},
            }))
        except Exception:
            pass

    try:
        while True:
            raw = await websocket.receive_text()

            # Heartbeat
            if raw.strip() == "ping":
                await websocket.send_text("pong")
                continue

            # Forward relay responses to ALL browsers in this group
            for client in list(group_clients.get(group, set())):
                try:
                    await client.send_text(raw)
                except Exception:
                    group_clients.get(group, set()).discard(client)

    except WebSocketDisconnect:
        log.info(f"[Relay] Disconnected: group={group[:12]}...")
    finally:
        group_relays.pop(group, None)

        # Notify browsers
        for client in group_clients.get(group, set()):
            try:
                await client.send_text(json.dumps({
                    "event": "relay_offline",
                    "data": {"group": group},
                }))
            except Exception:
                pass


# ── Static ──────────────────────────────────────────────────────────
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
    log.info("=" * 50)
    log.info("polly-bridge v0.4.0 — WebSocket Pairing Relay")
    log.info(f"  Port: {PORT}")
    log.info("  Chain: Browser → polly-bridge → local_relay → KissToy → phone → BLE → device")
    log.info("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=PORT)
