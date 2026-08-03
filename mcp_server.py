#!/usr/bin/env python3
"""
Polly MCP Server — KissToy Device Control (zero-dependency)
============================================================
Implements MCP JSON-RPC protocol over stdio using ONLY Python stdlib.
No pip install needed.

Tools:
  polly_init     — take over as master controller (needs fresh session_id)
  polly_control  — set motor speeds (m1=vibration, m3=suction)
  polly_stop     — emergency stop all motors

Configure in ~/.claude/.mcp.json:
  {"mcpServers": {"polly": {"command": "python", "args": [".../mcp_server.py"]}}}
"""

import json
import os
import sys
import urllib.request
import urllib.error

# ── Config ──────────────────────────────────────────────────────────
POLLY_BRIDGE = os.environ.get("POLLY_BRIDGE_URL", "https://polly-bridge.onrender.com")
GROUP = os.environ.get("POLLY_GROUP", "82e0ff7c8e3dabe932332e6ea65d272d")
DEVICE_ID = os.environ.get("POLLY_DEVICE_ID", "33")

SERVER_NAME = "polly-mcp"
SERVER_VERSION = "1.0.0"


def log(msg: str):
    """Log to stderr (stdout is for MCP protocol)."""
    print(f"[polly-mcp] {msg}", file=sys.stderr, flush=True)


# ── API Helpers ─────────────────────────────────────────────────────

def api_get(path: str) -> dict:
    """Call polly-bridge REST API."""
    url = f"{POLLY_BRIDGE}{path}"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return json.loads(resp.read())
    except Exception as e:
        return {"error": str(e)}


# ── Tools ───────────────────────────────────────────────────────────

TOOLS = [
    {
        "name": "polly_init",
        "description": "接管 Polly 设备主控权。需传入 KissToy 分享链接中的 session_id（id= 后面的值）。获取主控权后即可用 polly_control 控制设备。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "session_id": {
                    "type": "string",
                    "description": "KissToy 分享链接中 id= 后面的会话 ID"
                }
            },
            "required": ["session_id"]
        }
    },
    {
        "name": "polly_control",
        "description": "控制 Polly 设备的电机。m1 为震动（入体端），m3 为吮吸，强度 0-100 按 5 取整。m1 和 m3 都为 0 时停止所有电机。",
        "inputSchema": {
            "type": "object",
            "properties": {
                "m1": {
                    "type": "integer",
                    "description": "震动电机强度 0-100（按 5 取整：0/5/10/.../100）",
                    "minimum": 0,
                    "maximum": 100
                },
                "m3": {
                    "type": "integer",
                    "description": "吮吸电机强度 0-100（按 5 取整：0/5/10/.../100）",
                    "minimum": 0,
                    "maximum": 100
                }
            },
            "required": ["m1", "m3"]
        }
    },
    {
        "name": "polly_stop",
        "description": "紧急停止 — 立即关闭 Polly 所有电机。",
        "inputSchema": {
            "type": "object",
            "properties": {},
            "required": []
        }
    },
]


def call_polly_init(session_id: str) -> str:
    log(f"polly_init: session_id={session_id}")
    result = api_get(
        f"/api/become-controller?group={GROUP}&session_id={session_id}&device_id={DEVICE_ID}"
    )
    if result.get("ws_connected"):
        return json.dumps({
            "status": "ok",
            "message": "主控权已接管。设备就绪，可以用 polly_control 控制了。",
        }, ensure_ascii=False)
    else:
        return json.dumps({
            "status": "failed",
            "message": f"接管失败: {result.get('ws_error', result)}",
        }, ensure_ascii=False)


def call_polly_control(m1: int, m3: int) -> str:
    log(f"polly_control: m1={m1}, m3={m3}")
    result = api_get(
        f"/api/command?group={GROUP}&device_id={DEVICE_ID}&m1={m1}&m3={m3}"
    )
    if result.get("sent"):
        motors = result.get("motors", {})
        parts = []
        if motors.get("1", 0) > 0:
            parts.append(f"震动 {motors['1']}%")
        if motors.get("3", 0) > 0:
            parts.append(f"吮吸 {motors['3']}%")
        desc = " + ".join(parts) if parts else "已停止"
        return json.dumps({"status": "ok", "message": desc}, ensure_ascii=False)
    else:
        return json.dumps({
            "status": "failed",
            "message": "指令可能未送达。请用 polly_init 重新获取主控权。",
        }, ensure_ascii=False)


def call_polly_stop() -> str:
    log("polly_stop")
    result = api_get(
        f"/api/command?group={GROUP}&device_id={DEVICE_ID}&m1=0&m3=0"
    )
    if result.get("sent"):
        return json.dumps({"status": "ok", "message": "所有电机已停止"}, ensure_ascii=False)
    return json.dumps({"status": "failed", "message": "停止命令发送失败"}, ensure_ascii=False)


# ── MCP JSON-RPC Protocol ───────────────────────────────────────────

def send_response(id, result):
    """Write JSON-RPC response to stdout."""
    msg = json.dumps({"jsonrpc": "2.0", "id": id, "result": result})
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def send_error(id, code: int, message: str):
    """Write JSON-RPC error to stdout."""
    msg = json.dumps({
        "jsonrpc": "2.0", "id": id,
        "error": {"code": code, "message": message}
    })
    sys.stdout.write(msg + "\n")
    sys.stdout.flush()


def handle_request(req: dict):
    """Handle a single JSON-RPC request."""
    method = req.get("method", "")
    req_id = req.get("id")
    params = req.get("params", {})

    if method == "initialize":
        send_response(req_id, {
            "protocolVersion": "2024-11-05",
            "capabilities": {"tools": {}},
            "serverInfo": {"name": SERVER_NAME, "version": SERVER_VERSION},
        })
    elif method == "tools/list":
        send_response(req_id, {"tools": TOOLS})
    elif method == "tools/call":
        tool_name = params.get("name", "")
        arguments = params.get("arguments", {})
        try:
            if tool_name == "polly_init":
                text = call_polly_init(arguments["session_id"])
            elif tool_name == "polly_control":
                text = call_polly_control(
                    arguments.get("m1", 0),
                    arguments.get("m3", 0),
                )
            elif tool_name == "polly_stop":
                text = call_polly_stop()
            else:
                text = json.dumps({"error": f"Unknown tool: {tool_name}"})
            send_response(req_id, {"content": [{"type": "text", "text": text}]})
        except KeyError as e:
            send_response(req_id, {
                "content": [{"type": "text",
                    "text": json.dumps({"error": f"Missing required argument: {e}"})}]
            })
    elif method == "notifications/initialized":
        pass  # No response for notifications
    else:
        send_error(req_id, -32601, f"Method not found: {method}")


def main():
    log(f"Starting — bridge: {POLLY_BRIDGE}, group: {GROUP[:12]}...")

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            handle_request(req)
        except json.JSONDecodeError as e:
            log(f"JSON parse error: {e}")
        except Exception as e:
            log(f"Unexpected error: {e}")
            # Try to send error if we have an id
            try:
                send_error(req.get("id"), -32603, str(e))
            except Exception:
                pass


if __name__ == "__main__":
    main()
