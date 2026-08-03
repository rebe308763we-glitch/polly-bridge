"""
local_relay — bridges polly-bridge ↔ KissToy WebSocket
========================================================
Runs on YOUR local machine. Connects to:
  1. polly-bridge /ws/relay  — receives browser commands
  2. KissToy WebSocket       — forwards with correct Origin/IP

Why: KissToy server ignores commands from non-China IPs (e.g. Render US).
Your local machine (China IP) can relay successfully.

Usage:
  python local_relay.py <session_id>
  e.g.: python local_relay.py 756801
"""

import asyncio
import json
import logging
import sys
import time
import urllib.request

import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("local-relay")

POLLY_BRIDGE = "wss://polly-bridge.onrender.com"
KISSTOY_WS = "wss://api.app.knightjenay.cn/websocket-kisstoy"
KISSTOY_API = "https://api.app.knightjenay.cn"
GROUP = "82e0ff7c8e3dabe932332e6ea65d272d"


def call_binding(user_id: str):
    """Register this session before sending commands."""
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


async def main():
    session_id = sys.argv[1] if len(sys.argv) > 1 else "756801"

    print("=" * 55)
    print("  polly-bridge Local Relay")
    print(f"  Group:     {GROUP[:16]}...")
    print(f"  Session:   {session_id}")
    print(f"  Bridge:    {POLLY_BRIDGE}")
    print(f"  KissToy:   {KISSTOY_WS}?group=...")
    print("  Chain: Browser → polly-bridge → YOU → KissToy → phone → BLE")
    print("=" * 55)

    # 0. Call binding first
    call_binding(session_id)

    while True:
        polly_ws = None
        kisstoy_ws = None

        try:
            # 1. Connect to polly-bridge relay endpoint
            polly_url = f"{POLLY_BRIDGE}/ws/relay?group={GROUP}"
            polly_ws = await websockets.connect(polly_url, ping_interval=None)
            log.info("✓ Connected to polly-bridge relay")

            # 2. Connect to KissToy (URL: just ?group=, no id param)
            kisstoy_url = f"{KISSTOY_WS}?group={GROUP}"
            kisstoy_ws = await websockets.connect(
                kisstoy_url,
                ping_interval=None,
                additional_headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
                    "Origin": "https://api.app.knightjenay.cn",
                },
                open_timeout=15,
            )
            log.info(f"✓ Connected to KissToy ({kisstoy_ws.remote_address})")

            # ── Bidirectional relay ────────────────────────────

            async def polly_to_kisstoy():
                """Browser commands → KissToy"""
                async for raw in polly_ws:
                    if raw.strip() == "ping":
                        await polly_ws.send("pong")
                        continue
                    try:
                        msg = json.loads(raw)
                        motors = msg.get("data", {}).get("motors", {})
                        if motors:
                            log.info(f"[→] motors={motors}")
                    except Exception:
                        pass
                    try:
                        await kisstoy_ws.send(raw)
                    except Exception as e:
                        log.error(f"[→] Send error: {e}")

            async def kisstoy_to_polly():
                """KissToy responses → browser"""
                async for raw in kisstoy_ws:
                    if raw.strip() == "ping":
                        await kisstoy_ws.send("pong")
                        continue
                    try:
                        await polly_ws.send(raw)
                    except Exception:
                        pass

            async def heartbeat():
                while True:
                    await asyncio.sleep(10)
                    try:
                        await kisstoy_ws.send("ping")
                    except Exception:
                        break

            await asyncio.gather(polly_to_kisstoy(), kisstoy_to_polly(), heartbeat())

        except websockets.ConnectionClosed as e:
            log.warning(f"Connection closed: {e}")
        except Exception as e:
            log.error(f"Error: {type(e).__name__}: {e}")
        finally:
            for ws in [polly_ws, kisstoy_ws]:
                if ws:
                    try:
                        await ws.close()
                    except Exception:
                        pass

        log.info("Reconnecting in 3s...")
        await asyncio.sleep(3)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down")
