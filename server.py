"""
polly-bridge — KissToy WebSocket Relay
=======================================
Acts as a transparent WebSocket relay between controllers and the real
KissToy/KnightJenay WebSocket server.

Actual chain:
  Browser/Claude → polly-bridge (Render) → KissToy WS server → phone app → BLE → device

polly-bridge:
  1. Accepts browser connections at /websocket-kisstoy?group=...
  2. Relays to the real KissToy server: api.app.knightjenay.cn
  3. Forwards ping/pong and all control messages bidirectionally

KissToy protocol:
  Connect:  wss://api.app.knightjenay.cn/websocket-kisstoy?group=<hash>
  Command:  {"event":"control","data":{"target":"<group>","device_id":"33","motors":{"1":15}}}
  Heartbeat: "ping" ↔ "pong" every 10s
  Motors:   1 = vibrate (0-15), 3 = suction (0-10)
"""

import asyncio
import json
import logging
import os
import time
from typing import Optional

import websockets
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn

# ── Config ──────────────────────────────────────────────────────────
PORT = int(os.environ.get("PORT", "8000"))
KISSTOY_WS = os.environ.get("KISSTOY_WS", "wss://api.app.knightjenay.cn/websocket-kisstoy")
RATE_MAX = 120
RATE_WINDOW = 60

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polly-bridge")

# ── App ─────────────────────────────────────────────────────────────
app = FastAPI(title="polly-bridge", version="0.3.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── State ───────────────────────────────────────────────────────────
# group_id -> KissToy upstream WebSocket connection
group_upstreams: dict[str, websockets.WebSocketClientProtocol] = {}
# group_id -> set of browser downstream WebSockets
group_clients: dict[str, set[WebSocket]] = {}
# Rate limiting: key -> timestamps
rate_buckets: dict[str, list[float]] = {}


def check_rate(key: str) -> bool:
    now = time.time()
    bucket = rate_buckets.setdefault(key, [])
    bucket[:] = [t for t in bucket if now - t < RATE_WINDOW]
    if len(bucket) >= RATE_MAX:
        return False
    bucket.append(now)
    return True


async def get_or_create_upstream(group: str, session_id: str = "") -> websockets.WebSocketClientProtocol:
    """Get existing upstream connection for a group+session, or create a new one."""
    upstream_key = f"{group}:{session_id}" if session_id else group
    ws = group_upstreams.get(upstream_key)

    if ws is None or ws.closed:
        ws_url = f"{KISSTOY_WS}?group={group}"
        if session_id:
            ws_url += f"&id={session_id}"
        log.info(f"[Upstream] Connecting to KissToy: {ws_url[:100]}...")
        try:
            ws = await websockets.connect(
                ws_url,
                ping_interval=None,
                extra_headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Origin": "https://api.app.knightjenay.cn",
                },
                open_timeout=15,
            )
            group_upstreams[upstream_key] = ws
            log.info(f"[Upstream] ✓ Connected for {upstream_key[:30]}... ({ws.remote_address})")
        except Exception as e:
            log.error(f"[Upstream] Connection failed: {e}")
            raise

    return ws


async def forward_to_upstream(group: str, message: str, session_id: str = ""):
    """Send a message to the KissToy upstream server."""
    try:
        ws = await get_or_create_upstream(group, session_id)
        await ws.send(message)
        return True
    except Exception as e:
        log.error(f"[Forward ↑] Error: {e}")
        upstream_key = f"{group}:{session_id}" if session_id else group
        group_upstreams.pop(upstream_key, None)
        return False


# ── Models ──────────────────────────────────────────────────────────
class SessionInit(BaseModel):
    device_id: str
    group: Optional[str] = None
    user_id: Optional[str] = None
    lang: str = "zh"


# ── REST API ────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {
        "service": "polly-bridge",
        "version": "0.3.0",
        "mode": "KissToy WebSocket relay",
        "upstream": KISSTOY_WS,
        "active_groups": len(group_upstreams),
        "connected_clients": sum(len(c) for c in group_clients.values()),
    }


@app.get("/health")
async def health():
    return {
        "status": "healthy",
        "groups": len(group_upstreams),
        "clients": sum(len(c) for c in group_clients.values()),
    }


@app.get("/debug")
async def debug():
    """Debug endpoint showing upstream connection states."""
    upstreams = {}
    for group, ws in group_upstreams.items():
        upstreams[group[:12] + "..."] = {
            "open": not ws.closed if hasattr(ws, 'closed') else "unknown",
            "local": f"{ws.local_address}" if hasattr(ws, 'local_address') else "?",
            "remote": f"{ws.remote_address}" if hasattr(ws, 'remote_address') else "?",
        }
    clients = {}
    for group, cset in group_clients.items():
        clients[group[:12] + "..."] = len(cset)
    return {
        "upstream_kisstoy": KISSTOY_WS,
        "upstreams": upstreams,
        "clients": clients,
    }


@app.post("/api/session/init")
async def init_session(session: SessionInit, request: Request):
    """Create a control session, returns KissToy-compatible WebSocket URL."""
    group = session.group or "default"
    host = request.headers.get("host", "localhost")
    return {
        "ws_url": f"wss://{host}/websocket-kisstoy?group={group}",
        "group": group,
        "device_id": session.device_id,
        "upstream": KISSTOY_WS,
        "motors": {
            "1": {"name": "震动 (Vibrate)", "range": [0, 15]},
            "3": {"name": "吮吸 (Suction)", "range": [0, 10]},
        },
    }


# ── Main WebSocket Relay ────────────────────────────────────────────
@app.websocket("/websocket-kisstoy")
async def ws_relay(websocket: WebSocket, group: str = Query(...), id: str = Query(default="", alias="id")):
    """
    Browser-facing WebSocket. Relays all messages to/from the real
    KissToy server at api.app.knightjenay.cn.

    Passes group + id (session) through to upstream.
    Browser → polly-bridge → KissToy server → phone app → BLE → device
    """
    await websocket.accept()
    group_clients.setdefault(group, set()).add(websocket)
    client_ip = websocket.client.host if websocket.client else "?"
    session_id = id or ""
    upstream_key = f"{group}:{session_id}" if session_id else group
    log.info(f"[Client] Browser connected: {upstream_key[:30]}... from {client_ip}")

    # Ensure upstream is connected
    try:
        await get_or_create_upstream(group, session_id)
    except Exception:
        await websocket.send_text(json.dumps({
            "event": "error",
            "data": {"message": "Cannot reach KissToy upstream server"},
        }))
        group_clients.get(group, set()).discard(websocket)
        await websocket.close()
        return

    # Start upstream reader task for this group
    upstream = group_upstreams.get(upstream_key)
    upstream_messages: asyncio.Queue = asyncio.Queue()

    async def read_from_upstream():
        """Read messages from KissToy server, forward to all browsers in group."""
        try:
            async for raw in upstream:
                for client in list(group_clients.get(group, set())):
                    try:
                        await client.send_text(raw)
                    except Exception:
                        group_clients.get(group, set()).discard(client)
        except websockets.ConnectionClosed:
            log.info(f"[Upstream] KissToy connection closed for group {group[:12]}...")
            group_upstreams.pop(group, None)
        except Exception as e:
            log.error(f"[Upstream] Reader error: {e}")
            group_upstreams.pop(group, None)

    upstream_task = asyncio.create_task(read_from_upstream())

    try:
        while True:
            raw = await websocket.receive_text()

            # Heartbeat handling — local response to ping
            if raw.strip() == "ping":
                await websocket.send_text("pong")
                continue

            # Try to forward command to upstream
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                await websocket.send_text(json.dumps({
                    "event": "error", "data": {"message": "Invalid JSON"}
                }))
                continue

            event = msg.get("event", "")

            if event == "control":
                if not check_rate(f"{group}:{client_ip}"):
                    await websocket.send_text(json.dumps({
                        "event": "error",
                        "data": {"message": "Rate limit exceeded"},
                    }))
                    continue

                motors = msg.get("data", {}).get("motors", {})
                log.info(f"[Relay →] group={group[:12]}... motors={motors}")

                # Forward to KissToy upstream
                ok = await forward_to_upstream(group, raw, session_id)
                await websocket.send_text(json.dumps({
                    "event": "ack",
                    "data": {
                        "motors": motors,
                        "relayed": ok,
                        "message": "forwarded to KissToy" if ok else "upstream unavailable",
                    },
                }))

            elif event == "ping":
                await websocket.send_text(json.dumps({"event": "pong"}))

            else:
                # Forward unknown events too
                await forward_to_upstream(group, raw, session_id)

    except WebSocketDisconnect:
        log.info(f"[Client] Browser disconnected: group={group[:12]}...")
    except Exception as e:
        log.error(f"[Client] Error: {e}")
    finally:
        group_clients.get(group, set()).discard(websocket)
        if not group_clients.get(group):
            group_clients.pop(group, None)

        # Cancel upstream reader
        upstream_task.cancel()
        try:
            await upstream_task
        except asyncio.CancelledError:
            pass


# ── Upstream Keepalive ──────────────────────────────────────────────
@app.on_event("startup")
async def start_keepalive():
    asyncio.create_task(keepalive_monitor())


async def keepalive_monitor():
    """Send ping to KissToy upstream every 10s to keep connection alive."""
    while True:
        await asyncio.sleep(10)
        for group, ws in list(group_upstreams.items()):
            try:
                await ws.send("ping")
            except Exception:
                log.info(f"[Keepalive] Removing dead upstream for group {group[:12]}...")
                group_upstreams.pop(group, None)


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
    log.info("polly-bridge v0.3.0 — KissToy WebSocket Relay")
    log.info(f"  Upstream: {KISSTOY_WS}")
    log.info(f"  Port:     {PORT}")
    log.info("  Chain: Browser → polly-bridge → KissToy → phone → BLE → device")
    log.info("=" * 50)
    uvicorn.run(app, host="0.0.0.0", port=PORT)
