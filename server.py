"""
polly-bridge — KissToy Cloud Remote Relay
==========================================
Relays control commands to KissToy cloud server. Includes binding step.

Chain:
  Browser/Claude → polly-bridge (Render) → KissToy Cloud → phone app → BLE → device

Protocol (from kisstoy-cloud-remote):
  Binding: POST /kisstoy/remote-control/binding  {"id": "USER_ID"}
  WS:      wss://api.app.knightjenay.cn/websocket-kisstoy?group=GROUP
  Command: {"event":"control","data":{"target":"GROUP","device_id":"33","motors":{"1":60}}}
  Intensity: 0-100, quantized to steps of 5
"""

import asyncio
import json
import logging
import os
import time
import urllib.request

from fastapi import FastAPI, WebSocket, WebSocketDisconnect, Request, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn
import websockets

# ── Config ──────────────────────────────────────────────────────────
PORT = int(os.environ.get("PORT", "8000"))
KISSTOY_WS = os.environ.get("KISSTOY_WS", "wss://api.app.knightjenay.cn/websocket-kisstoy")
KISSTOY_API = os.environ.get("KISSTOY_API", "https://api.app.knightjenay.cn")
RATE_MAX = 120
RATE_WINDOW = 60

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("polly-bridge")

# ── App ─────────────────────────────────────────────────────────────
app = FastAPI(title="polly-bridge", version="0.5.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True,
                   allow_methods=["*"], allow_headers=["*"])

# ── State ───────────────────────────────────────────────────────────
group_upstreams: dict[str, websockets.WebSocketClientProtocol] = {}
group_relays: dict[str, WebSocket] = {}  # local_relay.py connections
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


def call_binding(user_id: str):
    """Register this remote session with the KissToy cloud server."""
    try:
        url = f"{KISSTOY_API}/kisstoy/remote-control/binding"
        body = json.dumps({"id": user_id}).encode()
        req = urllib.request.Request(url, data=body,
                                     headers={"Content-Type": "application/json"},
                                     method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
            log.info(f"[Binding] user_id={user_id} → {result}")
            return result
    except Exception as e:
        log.error(f"[Binding] Failed: {e}")
        return None


async def get_or_create_upstream(group: str, user_id: str = ""):
    """Create upstream to KissToy WebSocket. URL: ?group=GROUP only (no id)."""
    ws = group_upstreams.get(group)
    if ws is None or ws.closed:
        # Call binding first if user_id provided
        if user_id:
            call_binding(user_id)

        ws_url = f"{KISSTOY_WS}?group={group}"
        log.info(f"[Upstream] Connecting: {ws_url}")
        ws = await websockets.connect(ws_url, ping_interval=None, open_timeout=15)
        group_upstreams[group] = ws
        log.info(f"[Upstream] ✓ Connected ({ws.remote_address})")
    return ws


# ── REST ────────────────────────────────────────────────────────────
@app.get("/")
async def root():
    return {"service": "polly-bridge", "version": "0.5.0",
            "upstream": KISSTOY_WS, "groups": len(group_upstreams)}


@app.get("/health")
async def health():
    return {"status": "healthy", "groups": len(group_upstreams),
            "clients": sum(len(c) for c in group_clients.values())}


class SessionInit(BaseModel):
    device_id: str = "33"
    group: str = ""
    user_id: str = ""
    lang: str = "zh"


@app.get("/api/test-upstream")
async def test_upstream(group: str = Query(...), user_id: str = Query(default="")):
    """Diagnostic: test full upstream chain."""
    result = {"group": group, "user_id": user_id}

    # 1. Binding
    if user_id:
        binding = call_binding(user_id)
        result["binding"] = binding

    # 2. WebSocket connection
    try:
        ws = await get_or_create_upstream(group, user_id)
        result["ws_connected"] = True
        result["ws_remote"] = str(ws.remote_address) if hasattr(ws, 'remote_address') else "?"
    except Exception as e:
        result["ws_connected"] = False
        result["ws_error"] = str(e)
        return result

    # 3. Check online status
    try:
        await ws.send(json.dumps({"event": "online_status", "data": {"group": group}}))
        import asyncio as _asyncio
        resp = await _asyncio.wait_for(ws.recv(), timeout=5)
        result["online_response"] = json.loads(resp)
    except Exception as e:
        result["online_error"] = str(e)

    # 4. Send a test command
    try:
        test_cmd = json.dumps({
            "event": "control",
            "data": {
                "target": group,
                "device_id": "33",
                "motors": {"1": 10}
            }
        })
        await ws.send(test_cmd)
        result["test_command_sent"] = True
    except Exception as e:
        result["test_command_error"] = str(e)

    return result


@app.post("/api/session/init")
async def init_session(s: SessionInit, request: Request):
    host = request.headers.get("host", "localhost")
    # Call binding
    if s.user_id:
        call_binding(s.user_id)
    return {"ws_url": f"wss://{host}/websocket-kisstoy?group={s.group}&id={s.user_id}",
            "group": s.group, "device_id": s.device_id, "user_id": s.user_id}


# ── Browser WebSocket ───────────────────────────────────────────────
@app.websocket("/websocket-kisstoy")
async def ws_browser(websocket: WebSocket, group: str = Query(...),
                     id: str = Query(default="", alias="id")):
    """Browser connects. Commands forwarded to KissToy upstream."""
    await websocket.accept()
    group_clients.setdefault(group, set()).add(websocket)
    user_id = id or ""
    log.info(f"[Browser] group={group[:12]}... user_id={user_id}")

    # Ensure upstream
    try:
        upstream = await get_or_create_upstream(group, user_id)
    except Exception as e:
        await websocket.send_text(json.dumps({
            "event": "error", "data": {"message": f"Upstream failed: {e}"},
        }))
        group_clients[group].discard(websocket)
        return

    # Read from upstream → forward to browsers
    async def upstream_reader():
        try:
            async for raw in upstream:
                for client in list(group_clients.get(group, set())):
                    try:
                        await client.send_text(raw)
                    except Exception:
                        group_clients[group].discard(client)
        except websockets.ConnectionClosed:
            log.info(f"[Upstream] Closed for group={group[:12]}...")
            group_upstreams.pop(group, None)
        except Exception as e:
            log.error(f"[Upstream] Reader error: {e}")
            group_upstreams.pop(group, None)

    task = asyncio.create_task(upstream_reader())

    try:
        while True:
            raw = await websocket.receive_text()
            if raw.strip() == "ping":
                await websocket.send_text("pong")
                continue
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue

            event = msg.get("event", "")
            if event == "control":
                if not check_rate(f"{group}"):
                    await websocket.send_text(json.dumps({
                        "event": "error", "data": {"message": "Rate limit"},
                    }))
                    continue
                motors = msg.get("data", {}).get("motors", {})
                log.info(f"[→] motors={motors}")

                # Prefer local relay (China IP) over direct upstream (Render US IP)
                relay = group_relays.get(group)
                forwarded = False
                error_msg = ""

                if relay:
                    try:
                        await relay.send_text(raw)
                        forwarded = True
                        log.info(f"[→] via relay")
                    except Exception as e:
                        group_relays.pop(group, None)
                        error_msg = f"relay error: {e}"

                if not forwarded:
                    # Fall back to direct upstream
                    try:
                        await upstream.send(raw)
                        forwarded = True
                        log.info(f"[→] via upstream")
                    except Exception as e:
                        group_upstreams.pop(group, None)
                        error_msg = f"upstream error: {e}"

                await websocket.send_text(json.dumps({
                    "event": "ack",
                    "data": {"motors": motors, "relayed": forwarded,
                              "message": error_msg if not forwarded else "ok"},
                }))
    except WebSocketDisconnect:
        log.info(f"[Browser] Disconnected: group={group[:12]}...")
    finally:
        group_clients[group].discard(websocket)
        if not group_clients.get(group):
            group_clients.pop(group, None)
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


# ── Relay WebSocket (local_relay.py) ───────────────────────────────
@app.websocket("/ws/relay")
async def ws_relay(websocket: WebSocket, group: str = Query(...)):
    """local_relay.py connects here. Forwards browser commands to relay."""
    await websocket.accept()
    group_relays[group] = websocket
    log.info(f"[Relay] Connected: group={group[:12]}...")

    # Notify browsers
    for client in group_clients.get(group, set()):
        try:
            await client.send_text(json.dumps({
                "event": "relay_online", "data": {"group": group},
            }))
        except Exception:
            pass

    try:
        while True:
            raw = await websocket.receive_text()
            if raw.strip() == "ping":
                await websocket.send_text("pong")
                continue
            # Forward relay responses to browsers
            for client in list(group_clients.get(group, set())):
                try:
                    await client.send_text(raw)
                except Exception:
                    group_clients[group].discard(client)
    except WebSocketDisconnect:
        log.info(f"[Relay] Disconnected: group={group[:12]}...")
    finally:
        group_relays.pop(group, None)
        for client in group_clients.get(group, set()):
            try:
                await client.send_text(json.dumps({
                    "event": "relay_offline", "data": {"group": group},
                }))
            except Exception:
                pass


# ── Keepalive ───────────────────────────────────────────────────────
@app.on_event("startup")
async def start_keepalive():
    async def keepalive():
        while True:
            await asyncio.sleep(10)
            for group, ws in list(group_upstreams.items()):
                try:
                    await ws.send("ping")
                except Exception:
                    group_upstreams.pop(group, None)
    asyncio.create_task(keepalive())


# ── Static ──────────────────────────────────────────────────────────
import os as _os
_static_dir = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "static")
if _os.path.isdir(_static_dir):
    app.mount("/remote", StaticFiles(directory=_static_dir, html=True), name="static")


@app.get("/remote")
async def remote_control():
    fp = _os.path.join(_static_dir, "index.html")
    if _os.path.isfile(fp):
        return FileResponse(fp)
    return HTMLResponse("Not found", status_code=404)


if __name__ == "__main__":
    log.info("polly-bridge v0.5.0 — KissToy Cloud Relay")
    log.info(f"  Upstream: {KISSTOY_WS}")
    log.info(f"  Chain: Browser → polly-bridge → KissToy Cloud → phone → BLE → device")
    uvicorn.run(app, host="0.0.0.0", port=PORT)
