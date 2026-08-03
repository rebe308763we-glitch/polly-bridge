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
from fastapi.responses import HTMLResponse, FileResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
import uvicorn
import uuid
import websockets

# ── Config ──────────────────────────────────────────────────────────
PORT = int(os.environ.get("PORT", "8000"))
KISSTOY_WS = os.environ.get("KISSTOY_WS", "wss://api.app.knightjenay.cn/websocket-kisstoy")
KISSTOY_API = os.environ.get("KISSTOY_API", "https://api.app.knightjenay.cn")
DEFAULT_DEVICE_ID = os.environ.get("DEVICE_ID", "33")  # PLY5 = 19, override via env
DEFAULT_GROUP = os.environ.get("POLLY_GROUP", "82e0ff7c8e3dabe932332e6ea65d272d")
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


async def get_or_create_upstream(group: str, user_id: str = "", skip_binding: bool = False, force: bool = False):
    """Create upstream to KissToy WebSocket. pass user_id to become controller.
    Set force=True to disconnect existing WS and create a fresh one (for polly_init)."""
    if force:
        old = group_upstreams.pop(group, None)
        if old:
            try: await old.close()
            except Exception: pass
            log.info(f"[Upstream] Force-closed old connection for group {group[:12]}...")

    ws = group_upstreams.get(group)
    needs_new = ws is None
    if not needs_new:
        try:
            await asyncio.wait_for(ws.ping(), timeout=3)
        except Exception:
            needs_new = True
    if needs_new:
        # HTTP binding (optional — browser may do this via WS instead)
        if user_id and not skip_binding:
            call_binding(user_id)

        ws_url = f"{KISSTOY_WS}?group={group}&id={user_id}" if user_id else f"{KISSTOY_WS}?group={group}"
        log.info(f"[Upstream] Connecting: {ws_url}")
        ws = await websockets.connect(
            ws_url, ping_interval=None, open_timeout=15,
            additional_headers={
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                "Origin": "https://api.app.knightjenay.cn",
            },
        )
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
    device_id: str = DEFAULT_DEVICE_ID
    group: str = ""
    user_id: str = ""
    lang: str = "zh"


@app.get("/api/test-upstream")
async def test_upstream(group: str = Query(...), user_id: str = Query(default=""),
                         device_id: str = Query(default=DEFAULT_DEVICE_ID)):
    """Diagnostic: test full upstream chain."""
    result = {"group": group, "user_id": user_id, "device_id": device_id}

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
                "device_id": device_id,
                "motors": {"1": 10}
            }
        })
        await ws.send(test_cmd)
        result["test_command_sent"] = True
    except Exception as e:
        result["test_command_error"] = str(e)

    return result


@app.get("/api/become-controller")
async def become_controller(group: str = Query(...), session_id: str = Query(...),
                             device_id: str = Query(default=DEFAULT_DEVICE_ID)):
    """Try to become the controller by connecting WS with session_id, no HTTP binding.
    This tests whether the WS connection itself (with a fresh session id) is enough
    to create the master session, without needing the browser's HTTP binding call."""
    result = {"group": group, "session_id": session_id, "device_id": device_id,
              "approach": "WS with session_id, NO HTTP binding"}

    # 1. Connect WS with session_id, skip HTTP binding
    try:
        ws = await get_or_create_upstream(group, session_id, skip_binding=True)
        result["ws_connected"] = True
        result["ws_remote"] = str(ws.remote_address) if hasattr(ws, 'remote_address') else "?"
    except Exception as e:
        result["ws_connected"] = False
        result["ws_error"] = str(e)
        return result

    # 2. Check online status
    try:
        await ws.send(json.dumps({"event": "online_status", "data": {"group": group}}))
        import asyncio as _asyncio
        resp = await _asyncio.wait_for(ws.recv(), timeout=5)
        result["online_response"] = json.loads(resp)
    except Exception as e:
        result["online_error"] = str(e)

    # 3. Send test pulse (on then off)
    try:
        test_cmd = json.dumps({
            "event": "control",
            "data": {
                "target": group,
                "device_id": device_id,
                "motors": {"1": 10}
            }
        })
        await ws.send(test_cmd)
        result["test_command_sent"] = True
        # Auto-stop after a brief pulse
        await asyncio.sleep(0.5)
        stop_cmd = json.dumps({
            "event": "control",
            "data": {
                "target": group,
                "device_id": device_id,
                "motors": {"1": 0, "3": 0}
            }
        })
        await ws.send(stop_cmd)
    except Exception as e:
        result["test_command_error"] = str(e)

    return result


class MotorCommand(BaseModel):
    group: str = ""
    device_id: str = DEFAULT_DEVICE_ID
    motor1: int = 0   # vibration 0-100 (quantized to 5)
    motor3: int = 0   # suction 0-100 (quantized to 5)


@app.post("/api/command")
@app.get("/api/command")
async def send_command(group: str = Query(...), device_id: str = Query(default=DEFAULT_DEVICE_ID),
                        m1: int = Query(default=0), m3: int = Query(default=0)):
    """Send a motor command. Use this from Claude/any HTTP client.
    GET /api/command?group=...&m1=60&m3=0  →  vibration 60, suction 0
    GET /api/command?group=...&m1=0&m3=0   →  stop all"""
    result = {"group": group, "device_id": device_id, "motors": {}}

    # Quantize to 5
    def q(v):
        if v <= 0: return 0
        return max(5, round(v / 5) * 5)

    motors = {}
    m1_val = q(m1)
    m3_val = q(m3)
    if m1_val > 0 or m3_val > 0 or (m1 == 0 and m3 == 0):
        motors["1"] = m1_val
        motors["3"] = m3_val
    result["motors"] = motors

    try:
        ws = await get_or_create_upstream(group)
    except Exception as e:
        result["error"] = f"Upstream failed: {e}"
        return result

    try:
        cmd = json.dumps({
            "event": "control",
            "data": {"target": group, "device_id": device_id, "motors": motors}
        })
        await ws.send(cmd)
        result["sent"] = True
    except Exception as e:
        group_upstreams.pop(group, None)
        result["error"] = f"Send failed: {e}"
        result["sent"] = False

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


# ── MCP SSE + OAuth (for Claude Chat mobile/desktop) ─────────────────
import hashlib as _hashlib

def _randhex(n: int) -> str:
    return os.urandom(n).hex()

mcp_sessions: dict[str, asyncio.Queue] = {}
mcp_tokens: dict[str, dict] = {}       # token → {client_id, created_at}
mcp_clients: dict[str, str] = {}       # client_id → client_name
mcp_auth_codes: dict[str, dict] = {}   # code → {client_id, redirect_uri}

MCP_CLIENT_ID = os.environ.get("MCP_CLIENT_ID", "polly-bridge-client")
MCP_CLIENT_SECRET = os.environ.get("MCP_CLIENT_SECRET", _randhex(16))
MCP_REDIRECT_URI = os.environ.get("MCP_REDIRECT_URI", "http://localhost:0/callback")

log.info(f"[MCP-OAuth] client_id={MCP_CLIENT_ID}")

# Pre-generated static token — use with ?token=... to skip OAuth
MCP_STATIC_TOKEN = os.environ.get("MCP_POLLY_TOKEN", _randhex(24))
mcp_tokens[MCP_STATIC_TOKEN] = {"client_id": "static", "created_at": time.time()}
log.info(f"[MCP-OAuth] Static token: {MCP_STATIC_TOKEN[:12]}... (use ?token= in URL)")

MCP_TOOLS = [
    {
        "name": "polly_init",
        "description": "接管 Polly 设备主控权。需传入 KissToy 分享链接中的 session_id（id= 后面的值）。获取主控权后即可用 polly_control 控制设备。",
        "inputSchema": {
            "type": "object",
            "properties": {"session_id": {"type": "string", "description": "KissToy 分享链接中 id= 的值"}},
            "required": ["session_id"]
        }
    },
    {
        "name": "polly_control",
        "description": "控制 Polly 设备的电机。m1 为震动 0-100，m3 为吮吸 0-100，按 5 取整。都为 0 时停止。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "m1": {"type": "integer", "description": "震动强度 0-100", "minimum": 0, "maximum": 100},
                "m3": {"type": "integer", "description": "吮吸强度 0-100", "minimum": 0, "maximum": 100}
            },
            "required": ["m1", "m3"]
        }
    },
    {
        "name": "polly_stop",
        "description": "紧急停止 — 立即关闭 Polly 所有电机。",
        "inputSchema": {"type": "object", "properties": {}, "required": []}
    },
]


async def mcp_call_tool(name: str, arguments: dict) -> str:
    """Execute MCP tool call using internal functions."""
    if name == "polly_init":
        try:
            ws = await get_or_create_upstream(DEFAULT_GROUP, arguments["session_id"], skip_binding=True, force=True)
            return json.dumps({"status": "ok", "message": "主控权已接管，设备就绪！",
                               "ws": str(ws.remote_address)}, ensure_ascii=False)
        except Exception as e:
            return json.dumps({"status": "failed", "message": f"接管失败: {e}"}, ensure_ascii=False)

    elif name == "polly_control":
        m1 = arguments.get("m1", 0)
        m3 = arguments.get("m3", 0)
        try:
            ws = await get_or_create_upstream(DEFAULT_GROUP)
            cmd = json.dumps({
                "event": "control",
                "data": {"target": DEFAULT_GROUP, "device_id": DEFAULT_DEVICE_ID,
                         "motors": {"1": m1, "3": m3}}
            })
            await ws.send(cmd)
            parts = []
            if m1 > 0: parts.append(f"震动 {m1}%")
            if m3 > 0: parts.append(f"吮吸 {m3}%")
            return json.dumps({"status": "ok", "message": " + ".join(parts) if parts else "已停止"},
                              ensure_ascii=False)
        except Exception as e:
            return json.dumps({"status": "failed", "message": f"发送失败: {e}"}, ensure_ascii=False)

    elif name == "polly_stop":
        try:
            ws = await get_or_create_upstream(DEFAULT_GROUP)
            cmd = json.dumps({
                "event": "control",
                "data": {"target": DEFAULT_GROUP, "device_id": DEFAULT_DEVICE_ID,
                         "motors": {"1": 0, "3": 0}}
            })
            await ws.send(cmd)
            return json.dumps({"status": "ok", "message": "所有电机已停止"}, ensure_ascii=False)
        except Exception as e:
            return json.dumps({"status": "failed", "message": f"停止失败: {e}"}, ensure_ascii=False)

    return json.dumps({"error": f"Unknown tool: {name}"})


async def mcp_sse_event(sid: str, data: str):
    """Push an SSE event to a session."""
    q = mcp_sessions.get(sid)
    if q:
        await q.put(data)


# ── OAuth Endpoints ───────────────────────────────────────────────

@app.get("/mcp/get-token")
async def mcp_get_token():
    """Generate a permanent token for simple auth (no OAuth needed)."""
    token = _randhex(24)
    mcp_tokens[token] = {"client_id": "manual", "created_at": time.time()}
    log.info(f"[MCP-OAuth] Manual token generated: {token[:12]}...")
    return {
        "token": token,
        "usage": f"https://polly-bridge.onrender.com/mcp/sse?token={token}",
        "note": "Add this to Claude Chat MCP Server URL. No OAuth needed."
    }


@app.get("/.well-known/oauth-protected-resource/{path:path}")
async def oauth_resource_metadata(request: Request, path: str):
    """OAuth 2.0 Protected Resource Metadata (RFC 9728) — tells Claude Chat how to auth."""
    base = str(request.base_url).rstrip("/")
    return {
        "resource": f"{base}/mcp/{path}",
        "authorization_servers": [base],
        "bearer_methods_supported": ["header"],
        "scopes_supported": ["mcp"],
    }


@app.get("/.well-known/oauth-authorization-server")
async def oauth_discovery(request: Request):
    """OAuth 2.0 Authorization Server Metadata (RFC 8414)."""
    base = str(request.base_url).rstrip("/")
    return {
        "issuer": base,
        "authorization_endpoint": f"{base}/oauth/authorize",
        "token_endpoint": f"{base}/oauth/token",
        "registration_endpoint": f"{base}/oauth/register",
        "response_types_supported": ["code"],
        "grant_types_supported": ["authorization_code", "refresh_token"],
        "code_challenge_methods_supported": ["S256"],
        "token_endpoint_auth_methods_supported": ["none"],
        "scopes_supported": ["mcp"],
    }


# ── /oauth/ aliases (Claude Chat expects these paths) ──────────────

@app.post("/oauth/register")
async def oauth_register_alias(request: Request):
    """Alias for /mcp/register"""
    body = await request.json() if request.headers.get("content-type") == "application/json" else {}
    client_name = body.get("client_name", "claude-chat")
    redirect_uris = body.get("redirect_uris", ["http://localhost:0/callback"])
    now_ts = int(time.time())
    client_id = _hashlib.sha256(f"{client_name}:{_randhex(8)}".encode()).hexdigest()[:32]
    client_secret = _randhex(24)
    mcp_clients[client_id] = client_name
    log.info(f"[OAuth] Registered: {client_id[:12]}... ({client_name})")
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "client_id_issued_at": now_ts,
        "client_secret_expires_at": 0,
        "client_name": client_name,
        "redirect_uris": redirect_uris,
        "grant_types": ["authorization_code", "refresh_token"],
        "response_types": ["code"],
        "token_endpoint_auth_method": "none",
    }

@app.get("/oauth/authorize")
async def oauth_authorize_alias(
    client_id: str = Query(...), redirect_uri: str = Query(...),
    response_type: str = Query(default="code"), state: str = Query(default=""),
    scope: str = Query(default=""), code_challenge: str = Query(default=""),
    code_challenge_method: str = Query(default=""),
):
    code = _randhex(16)
    mcp_auth_codes[code] = {"client_id": client_id, "redirect_uri": redirect_uri}
    log.info(f"[OAuth] Auth code: {code[:12]}...")
    sep = "&" if "?" in redirect_uri else "?"
    redirect_url = f"{redirect_uri}{sep}code={code}"
    if state: redirect_url += f"&state={state}"
    from starlette.responses import RedirectResponse
    return RedirectResponse(url=redirect_url, status_code=302)

@app.post("/oauth/token")
async def oauth_token_alias(request: Request):
    try: body = await request.json()
    except:
        try: body = dict(await request.form())
        except: return {"error": "invalid_request"}
    grant_type = body.get("grant_type", "")
    code = body.get("code", "")
    client_id = body.get("client_id", "")
    code_verifier = body.get("code_verifier", "")
    if grant_type == "authorization_code":
        auth = mcp_auth_codes.pop(code, None)
        if not auth: return {"error": "invalid_grant"}
        # Accept if client_id matches or if no client was specified
        token = _randhex(24)
        mcp_tokens[token] = {"client_id": auth.get("client_id", client_id), "created_at": time.time()}
        log.info(f"[OAuth] Token issued")
        return {"access_token": token, "token_type": "bearer", "expires_in": 86400}
    elif grant_type == "refresh_token":
        refresh = body.get("refresh_token", "")
        old = mcp_tokens.pop(refresh, None)
        if not old: return {"error": "invalid_grant"}
        token = _randhex(24)
        mcp_tokens[token] = old
        return {"access_token": token, "token_type": "bearer", "expires_in": 86400}
    return {"error": "unsupported_grant_type"}


@app.post("/mcp/register")
async def mcp_register(request: Request):
    """Dynamic Client Registration — Claude Chat registers here."""
    try:
        body = await request.json()
        client_name = body.get("client_name", "claude-chat")
        redirect_uris = body.get("redirect_uris", ["http://localhost:0/callback"])
    except Exception:
        client_name = "claude-chat"
        redirect_uris = ["http://localhost:0/callback"]

    now_ts = int(time.time())
    client_id = _hashlib.sha256(f"{client_name}:{_randhex(8)}".encode()).hexdigest()[:32]
    client_secret = _randhex(24)
    mcp_clients[client_id] = client_name
    log.info(f"[MCP-OAuth] Registered client: {client_id[:12]}... ({client_name})")
    return {
        "client_id": client_id,
        "client_secret": client_secret,
        "client_id_issued_at": now_ts,
        "client_secret_expires_at": 0,  # never expires
        "client_name": client_name,
        "redirect_uris": redirect_uris,
        "grant_types": ["authorization_code", "refresh_token"],
        "token_endpoint_auth_method": "none",
    }


@app.get("/mcp/authorize")
async def mcp_authorize(
    client_id: str = Query(...),
    redirect_uri: str = Query(...),
    response_type: str = Query(default="code"),
    state: str = Query(default=""),
    scope: str = Query(default=""),
):
    """OAuth authorization endpoint — auto-approves for personal use."""
    # Accept any client (no validation — personal server, Render loses in-memory state)
    code = _randhex(16)
    mcp_auth_codes[code] = {"client_id": client_id, "redirect_uri": redirect_uri}
    log.info(f"[MCP-OAuth] Auth code issued: {code[:12]}...")

    # Auto-redirect back with code
    sep = "&" if "?" in redirect_uri else "?"
    redirect_url = f"{redirect_uri}{sep}code={code}"
    if state:
        redirect_url += f"&state={state}"
    from starlette.responses import RedirectResponse
    return RedirectResponse(url=redirect_url, status_code=302)


@app.post("/mcp/token")
async def mcp_token(request: Request):
    """Token endpoint — exchange auth code for access token."""
    try:
        body = await request.json()
    except Exception:
        try:
            form = await request.form()
            body = dict(form)
        except Exception:
            return {"error": "invalid_request"}

    grant_type = body.get("grant_type", "")
    code = body.get("code", "")

    if grant_type == "authorization_code":
        auth = mcp_auth_codes.pop(code, None)
        if not auth:
            return {"error": "invalid_grant", "error_description": "Invalid or expired code"}
        token = _randhex(24)
        mcp_tokens[token] = {"client_id": auth["client_id"], "created_at": time.time()}
        log.info(f"[MCP-OAuth] Token issued for {auth['client_id'][:12]}...")
        return {
            "access_token": token,
            "token_type": "bearer",
            "expires_in": 86400,
        }
    elif grant_type == "refresh_token":
        refresh = body.get("refresh_token", "")
        old = mcp_tokens.pop(refresh, None)
        if not old:
            return {"error": "invalid_grant"}
        token = _randhex(24)
        mcp_tokens[token] = old
        return {"access_token": token, "token_type": "bearer", "expires_in": 86400}

    return {"error": "unsupported_grant_type"}


# ── SSE (with OAuth) ─────────────────────────────────────────────

@app.get("/mcp/sse")
async def mcp_sse(request: Request):
    """SSE endpoint with OAuth Bearer token support."""
    # Check for Bearer token
    auth_header = request.headers.get("Authorization", "")
    token = None
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
    # Also check query param (fallback)
    if not token:
        token = request.query_params.get("token", "")

    # Debug: allow noauth=1 to bypass auth for testing
    noauth = request.query_params.get("noauth", "")
    if noauth != "1" and (not token or token not in mcp_tokens):
        # RFC 9728 + RFC 6750: return 401 with WWW-Authenticate pointing to resource metadata
        base = str(request.base_url).rstrip("/")
        from fastapi.responses import JSONResponse
        return JSONResponse(
            status_code=401,
            content={
                "error": "Unauthorized",
                "resource_metadata": f"{base}/.well-known/oauth-protected-resource/mcp/sse"
            },
            headers={
                "WWW-Authenticate": f'Bearer resource_metadata="{base}/.well-known/oauth-protected-resource/mcp/sse"'
            }
        )

    sid = uuid.uuid4().hex[:12]
    q: asyncio.Queue = asyncio.Queue()
    mcp_sessions[sid] = q
    client_info = mcp_tokens.get(token, {"client_id": "anonymous"})
    log.info(f"[MCP-SSE] Client connected: {sid} (client={client_info['client_id'][:12]}...)")

    async def event_stream():
        try:
            yield f"event: endpoint\ndata: /mcp/message?session_id={sid}\n\n"
            while True:
                try:
                    data = await asyncio.wait_for(q.get(), timeout=30)
                    yield f"event: message\ndata: {data}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        except asyncio.CancelledError:
            pass
        finally:
            mcp_sessions.pop(sid, None)
            log.info(f"[MCP-SSE] Client disconnected: {sid}")

    return StreamingResponse(event_stream(), media_type="text/event-stream")


@app.post("/mcp/message")
async def mcp_message(request: Request, session_id: str = Query(...)):
    """Receive JSON-RPC messages from Claude Chat MCP client."""
    try:
        body = await request.json()
    except Exception:
        return {"error": "Invalid JSON"}

    method = body.get("method", "")
    req_id = body.get("id")
    params = body.get("params", {})

    log.info(f"[MCP-MSG] sid={session_id[:8]}... method={method}")

    if method == "initialize":
        result = {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "polly-bridge", "version": "0.5.0"},
        }
    elif method == "tools/list":
        result = {"tools": MCP_TOOLS}
    elif method == "tools/call":
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        text = await mcp_call_tool(tool_name, arguments)
        result = {"content": [{"type": "text", "text": text}]}
    elif method == "notifications/initialized":
        return {}  # No response
    else:
        result = {"error": {"code": -32601, "message": f"Unknown method: {method}"}}

    response = {"jsonrpc": "2.0", "id": req_id, "result": result}
    await mcp_sse_event(session_id, json.dumps(response))
    return {"status": "ok"}


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
