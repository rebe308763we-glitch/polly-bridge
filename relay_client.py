"""
polly-bridge Relay Client (KissToy Protocol)
=============================================
Runs on the local machine. Connects to polly-bridge via WSS and
forwards motor commands to Intiface Central (Buttplug.io) over BLE.

Motor mapping (KissToy → Buttplug):
  Motor 1: vibration (入体端), range 0-15 → intensity 0.0-1.0
  Motor 3: suction (吮吸), range 0-10 → oscillate intensity 0.0-1.0

Supports multiple devices — maps motors to available Buttplug devices.

Usage:
  pip install -r relay_requirements.txt
  python relay_client.py \
    --server wss://polly-bridge.onrender.com \
    --group 82e0ff7c8e3dabe932332e6ea65d272d \
    --token YOUR_RELAY_SECRET
"""

import asyncio
import json
import logging
import argparse
import sys
import time
import math
from typing import Optional

import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("relay-client")

# Motor value ranges (from KissToy protocol)
MOTOR_RANGES = {
    "1": 15,   # vibration
    "3": 10,   # suction
}


class RelayClient:
    def __init__(
        self,
        server_url: str,
        group: str,
        token: str,
        intiface_url: str = "ws://127.0.0.1:12345",
        device_mapping: Optional[dict] = None,
    ):
        self.server_url = server_url.rstrip("/")
        self.group = group
        self.token = token
        self.intiface_url = intiface_url
        self.client = None  # Buttplug client
        self.devices: list = []
        # Custom motor → device mapping: {"1": 0, "3": 1} (motor_id → device_index)
        self.motor_device_map = device_mapping or {}

    async def connect_intiface(self):
        """Connect to Intiface Central via Buttplug.io."""
        try:
            from buttplug import Client, WebsocketConnector
            connector = WebsocketConnector(self.intiface_url)
            self.client = Client("polly-relay", connector)
            await self.client.connect()
            log.info(f"✓ Connected to Intiface at {self.intiface_url}")
        except ImportError:
            log.error("buttplug-py not installed. Run: pip install buttplug-py")
            raise
        except Exception as e:
            log.error(f"Intiface connection failed: {e}")
            raise

    async def scan_devices(self):
        """Discover BLE devices via Intiface."""
        log.info("Scanning for devices...")
        await self.client.start_scanning()
        await asyncio.sleep(4)
        await self.client.stop_scanning()

        self.devices = list(self.client.devices)
        if not self.devices:
            log.warning("⚠ No devices found. Make sure:")
            log.warning("  1. Intiface Central is in Engine mode (port 12345)")
            log.warning("  2. Device is powered on and in range")
            return []

        log.info(f"Found {len(self.devices)} device(s):")
        for i, d in enumerate(self.devices):
            caps = []
            if d.vibrate_attributes:
                caps.append(f"vibrate({len(d.vibrate_attributes)} motor(s))")
            if hasattr(d, 'oscillate_attributes') and d.oscillate_attributes:
                caps.append(f"oscillate({len(d.oscillate_attributes)})")
            if hasattr(d, 'rotate_attributes') and d.rotate_attributes:
                caps.append(f"rotate({len(d.rotate_attributes)})")
            log.info(f"  [{i}] {d.name} — {', '.join(caps) if caps else 'basic'}")

        # Auto-map: if motor_device_map is empty, set defaults
        if not self.motor_device_map:
            if len(self.devices) >= 1:
                self.motor_device_map["1"] = 0  # first device = motor 1 (vibrate)
            if len(self.devices) >= 2:
                self.motor_device_map["3"] = 1  # second device = motor 3 (suction)
            elif len(self.devices) == 1:
                self.motor_device_map["3"] = 0  # same device for both motors

        log.info(f"Motor→Device mapping: {self.motor_device_map}")
        return self.devices

    async def send_motor_command(self, motors: dict):
        """
        Execute motor commands via Buttplug.

        motors: {"1": 15, "3": 5} — raw KissToy values
        """
        if not self.client or not self.devices:
            log.warning("No device connected")
            return

        action_log = []

        for motor_id, raw_value in motors.items():
            max_val = MOTOR_RANGES.get(motor_id, 15)
            intensity = max(0.0, min(1.0, int(raw_value) / max_val))
            device_idx = self.motor_device_map.get(motor_id)

            if device_idx is None or device_idx >= len(self.devices):
                log.warning(f"Motor {motor_id}: no device mapped")
                continue

            device = self.devices[device_idx]

            try:
                if motor_id == "1":
                    # Vibration motor (入体端)
                    await device.vibrate(intensity)
                    action_log.append(f"M1(vibrate)={intensity:.2f}")

                elif motor_id == "3":
                    # Suction motor (吮吸)
                    # Try oscillate first (closest to suction), fall back to vibrate
                    if hasattr(device, 'oscillate') and device.oscillate_attributes:
                        await device.oscillate(intensity)
                        action_log.append(f"M3(suction/osc)={intensity:.2f}")
                    else:
                        # Suction mapped to vibrate if oscillate not available
                        await device.vibrate(intensity * 0.7)  # slightly gentler
                        action_log.append(f"M3(suction→vibe)={intensity:.2f}")

                else:
                    # Unknown motor — default to vibrate
                    await device.vibrate(intensity)
                    action_log.append(f"M{motor_id}(vibe)={intensity:.2f}")

            except Exception as e:
                log.error(f"Motor {motor_id} command failed: {e}")

        if action_log:
            log.info(f"  Motors: {', '.join(action_log)}")

    async def stop_all(self):
        """Stop all motors on all devices."""
        if not self.client:
            return
        for device in self.devices:
            try:
                await device.stop()
            except Exception:
                pass
        log.info("  All motors stopped")

    async def run(self):
        """Main loop."""
        ws_url = f"{self.server_url}/ws/relay?group={self.group}&token={self.token}"
        log.info(f"polly-bridge Relay Client")
        log.info(f"  Server: {self.server_url}")
        log.info(f"  Group:  {self.group[:16]}...")
        log.info(f"  Motor ranges: M1=0-15 (vibrate), M3=0-10 (suction)")

        # Connect to Intiface
        intiface_ok = False
        while not intiface_ok:
            try:
                await self.connect_intiface()
                await self.scan_devices()
                intiface_ok = True
            except Exception:
                log.warning("Intiface not available. Retrying in 5s...")
                await asyncio.sleep(5)

        # Connect to polly-bridge
        while True:
            try:
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                    log.info("✓ Connected to polly-bridge server")
                    current_motors = {}

                    # Send device info to server
                    device_list = []
                    for i, d in enumerate(self.devices):
                        caps = []
                        if d.vibrate_attributes:
                            caps.append("vibrate")
                        if hasattr(d, 'oscillate_attributes') and d.oscillate_attributes:
                            caps.append("oscillate")
                        device_list.append({"index": i, "name": d.name, "capabilities": caps})

                    await ws.send(json.dumps({
                        "type": "device_info",
                        "group": self.group,
                        "devices": device_list,
                        "motor_map": self.motor_device_map,
                        "motor_ranges": MOTOR_RANGES,
                        "timestamp": time.time(),
                    }))

                    async for raw in ws:
                        try:
                            msg = json.loads(raw)
                        except json.JSONDecodeError:
                            continue

                        msg_type = msg.get("type", "")

                        if msg_type == "ping":
                            await ws.send(json.dumps({"type": "pong", "timestamp": time.time()}))

                        elif msg_type == "command":
                            motors = msg.get("motors", {})
                            if motors:
                                current_motors = motors
                                await self.send_motor_command(motors)

                            # Send status back
                            await ws.send(json.dumps({
                                "type": "status",
                                "group": self.group,
                                "motors": current_motors,
                                "online": True,
                                "timestamp": time.time(),
                            }))

                        elif msg_type == "controller_online":
                            log.info("  Browser controller connected — ready for commands")

                        elif msg_type == "controller_offline":
                            log.info("  Browser controller disconnected — stopping motors")
                            await self.stop_all()
                            current_motors = {}

            except websockets.ConnectionClosed:
                log.warning("Server connection lost. Reconnecting in 5s...")
                await asyncio.sleep(5)
            except Exception as e:
                log.error(f"Connection error: {e}. Reconnecting in 5s...")
                await asyncio.sleep(5)


def main():
    parser = argparse.ArgumentParser(
        description="polly-bridge Relay Client (KissToy Protocol)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python relay_client.py --server wss://polly-bridge.onrender.com --group abc123 --token secret
  python relay_client.py --server ws://localhost:8000 --group test --token dev --intiface ws://localhost:12345
        """,
    )
    parser.add_argument("--server", required=True, help="polly-bridge server URL (wss://...)")
    parser.add_argument("--group", required=True, help="Group hash (from KissToy share link)")
    parser.add_argument("--token", required=True, help="RELAY_SECRET from Render env vars")
    parser.add_argument("--intiface", default="ws://127.0.0.1:12345", help="Intiface WebSocket URL")
    parser.add_argument("--motor-map", default=None, help="Motor→Device mapping JSON, e.g. '{\"1\":0,\"3\":1}'")
    args = parser.parse_args()

    device_mapping = None
    if args.motor_map:
        device_mapping = json.loads(args.motor_map)

    client = RelayClient(
        server_url=args.server,
        group=args.group,
        token=args.token,
        intiface_url=args.intiface,
        device_mapping=device_mapping,
    )

    try:
        asyncio.run(client.run())
    except KeyboardInterrupt:
        log.info("Shutting down...")
        # Try to stop all motors on exit
        asyncio.run(client.stop_all())
        sys.exit(0)


if __name__ == "__main__":
    main()
