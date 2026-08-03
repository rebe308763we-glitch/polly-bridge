#!/usr/bin/env python3
"""
Polly MCP Server — KissToy Device Control via MCP Protocol
===========================================================
Exposes polly-bridge control as MCP tools. Claude Chat/Desktop/Code
can call these tools conversationally.

Tools:
  polly_init     — take over as master controller (needs fresh share-link session_id)
  polly_control  — set motor speeds (m1=vibration, m3=suction)
  polly_stop     — emergency stop all motors

Usage (stdio transport):
  python mcp_server.py

Configure in claude_desktop_config.json or .claude/settings.local.json:
  {
    "mcpServers": {
      "polly": {
        "command": "python",
        "args": ["C:/Users/kk/polly-bridge/mcp_server.py"]
      }
    }
  }
"""

import json
import logging
import os
import urllib.request
import urllib.error
from typing import Optional

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent

# ── Config ──────────────────────────────────────────────────────────
POLLY_BRIDGE = os.environ.get("POLLY_BRIDGE_URL", "https://polly-bridge.onrender.com")
GROUP = os.environ.get("POLLY_GROUP", "82e0ff7c8e3dabe932332e6ea65d272d")
DEVICE_ID = os.environ.get("POLLY_DEVICE_ID", "33")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [MCP] %(message)s")
log = logging.getLogger("polly-mcp")


# ── API Helpers ─────────────────────────────────────────────────────

def api_get(path: str) -> dict:
    """Call polly-bridge REST API."""
    url = f"{POLLY_BRIDGE}{path}"
    try:
        with urllib.request.urlopen(url, timeout=15) as resp:
            return json.loads(resp.read())
    except urllib.error.URLError as e:
        return {"error": f"Cannot reach polly-bridge: {e}"}
    except Exception as e:
        return {"error": str(e)}


# ── Tools ───────────────────────────────────────────────────────────

async def polly_init(session_id: str) -> str:
    """Take over as the master controller.

    Call this FIRST with a fresh session_id from the KissToy share link.
    The share link looks like:
      https://api.app.knightjenay.cn/kisstoy/remote/#/?device_id=33&group=...&id=SESSION_ID&lang=zh
    Extract the id= value and pass it here.

    Returns status — if ws_connected is true, you're now in control.
    """
    log.info(f"polly_init: session_id={session_id}")
    result = api_get(
        f"/api/become-controller?group={GROUP}&session_id={session_id}&device_id={DEVICE_ID}"
    )
    if result.get("ws_connected"):
        return json.dumps({
            "status": "ok",
            "message": f"Master controller activated. Device ready.",
            "details": result
        }, ensure_ascii=False)
    else:
        return json.dumps({
            "status": "failed",
            "message": f"Could not become controller.",
            "error": result.get("ws_error", str(result))
        }, ensure_ascii=False)


async def polly_control(m1: int = 0, m3: int = 0) -> str:
    """Send motor commands to the Polly device.

    Args:
        m1: Vibration motor intensity (0-100, quantized to steps of 5)
        m3: Suction motor intensity (0-100, quantized to steps of 5)

    Both m1=0 and m3=0 stops all motors.
    """
    log.info(f"polly_control: m1={m1}, m3={m3}")
    result = api_get(
        f"/api/command?group={GROUP}&device_id={DEVICE_ID}&m1={m1}&m3={m3}"
    )
    if result.get("sent"):
        motors = result.get("motors", {})
        m1_val = motors.get("1", 0)
        m3_val = motors.get("3", 0)
        parts = []
        if m1_val > 0:
            parts.append(f"震动 {m1_val}%")
        if m3_val > 0:
            parts.append(f"吮吸 {m3_val}%")
        desc = " + ".join(parts) if parts else "已停止"
        return json.dumps({"status": "ok", "message": desc, "motors": motors}, ensure_ascii=False)
    else:
        return json.dumps({
            "status": "failed",
            "message": "Command may not have reached device. Try polly_init with a fresh session_id.",
            "error": result.get("error", str(result))
        }, ensure_ascii=False)


async def polly_stop() -> str:
    """Emergency stop — immediately stop all motors."""
    log.info("polly_stop")
    result = api_get(
        f"/api/command?group={GROUP}&device_id={DEVICE_ID}&m1=0&m3=0"
    )
    if result.get("sent"):
        return json.dumps({"status": "ok", "message": "所有电机已停止"}, ensure_ascii=False)
    else:
        return json.dumps({"status": "failed", "message": "停止命令发送失败"}, ensure_ascii=False)


# ── MCP Server ──────────────────────────────────────────────────────

app = Server("polly-mcp")


@app.list_tools()
async def list_tools() -> list[Tool]:
    return [
        Tool(
            name="polly_init",
            description="接管 Polly 设备主控权。需传入 KissToy 分享链接中的 session_id（id= 后面的值）。连接成功后即可用 polly_control 控制设备。",
            inputSchema={
                "type": "object",
                "properties": {
                    "session_id": {
                        "type": "string",
                        "description": "KissToy 分享链接中 id= 后面的会话 ID，每次生成新链接时变化"
                    }
                },
                "required": ["session_id"]
            }
        ),
        Tool(
            name="polly_control",
            description="控制 Polly 设备的电机。m1 为震动（入体端），m3 为吮吸，强度 0-100 按 5 取整。都为 0 时停止。",
            inputSchema={
                "type": "object",
                "properties": {
                    "m1": {
                        "type": "integer",
                        "description": "震动电机强度 0-100，按 5 取整（0/5/10/.../100）",
                        "minimum": 0,
                        "maximum": 100
                    },
                    "m3": {
                        "type": "integer",
                        "description": "吮吸电机强度 0-100，按 5 取整（0/5/10/.../100）",
                        "minimum": 0,
                        "maximum": 100
                    }
                },
                "required": ["m1", "m3"]
            }
        ),
        Tool(
            name="polly_stop",
            description="紧急停止 — 立即关闭 Polly 所有电机。",
            inputSchema={
                "type": "object",
                "properties": {},
                "required": []
            }
        ),
    ]


@app.call_tool()
async def call_tool(name: str, arguments: dict) -> list[TextContent]:
    if name == "polly_init":
        result = await polly_init(arguments["session_id"])
    elif name == "polly_control":
        result = await polly_control(
            arguments.get("m1", 0),
            arguments.get("m3", 0)
        )
    elif name == "polly_stop":
        result = await polly_stop()
    else:
        result = json.dumps({"error": f"Unknown tool: {name}"})
    return [TextContent(type="text", text=result)]


# ── Entry Point ─────────────────────────────────────────────────────

async def main():
    log.info(f"Polly MCP Server starting — bridge: {POLLY_BRIDGE}, group: {GROUP[:12]}...")
    async with stdio_server() as (read_stream, write_stream):
        await app.run(read_stream, write_stream, app.create_initialization_options())


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
