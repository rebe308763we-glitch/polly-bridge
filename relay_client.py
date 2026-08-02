"""
polly-bridge Relay Client (KissToy Protocol)
=============================================
Runs on the local machine. Connects to polly-bridge via WSS and
forwards motor commands to Intiface Central (Buttplug.io) over BLE.

Uses buttplug-py 0.2.0 (actual API).

Motor mapping (KissToy → Buttplug):
  Motor 1: vibration (入体端), range 0-15 → ScalarCmd actuator_type="Vibrate"
  Motor 3: suction (吮吸), range 0-10 → ScalarCmd actuator_type="Oscillate"

Usage:
  pip install websockets buttplug-py
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
from typing import Optional

import websockets
from buttplug import Client, WebsocketConnector
from buttplug.messages.v3 import Scalar, ScalarCmd, StopDeviceCmd

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("relay-client")

# Motor value ranges (from KissToy protocol)
MOTOR_CONFIG = {
    "1": {"max": 15, "actuator_type": "Vibrate",  "label": "震动 (Vibrate)"},
    "3": {"max": 10, "actuator_type": "Oscillate", "label": "吮吸 (Suction/Oscillate)"},
}


class RelayClient:
    def __init__(
        self,
        server_url: str,
        group: str,
        token: str,
        intiface_url: str = "ws://127.0.0.1:12345",
        motor_device_map: Optional[dict] = None,
    ):
        self.server_url = server_url.rstrip("/")
        self.group = group
        self.token = token
        self.intiface_url = intiface_url
        self.client: Optional[Client] = None
        self.devices: list = []
        # motor_id -> {"device_index": int, "actuator_index": int}
        self.motor_map: dict[str, dict] = {}
        # custom override
        self.custom_mapping = motor_device_map or {}

    async def connect_intiface(self):
        """Connect to Intiface Central via Buttplug.io."""
        connector = WebsocketConnector(self.intiface_url)
        self.client = Client("polly-relay", connector)
        await self.client.connect()
        log.info(f"✓ Connected to Intiface at {self.intiface_url}")

    async def scan_devices(self):
        """Discover BLE devices via Intiface."""
        log.info("Scanning for devices...")
        await self.client.start_scanning()
        await asyncio.sleep(4)
        await self.client.stop_scanning()

        self.devices = list(self.client.devices)
        if not self.devices:
            log.warning("⚠ No devices found.")
            return []

        log.info(f"Found {len(self.devices)} device(s):")
        for i, d in enumerate(self.devices):
            acts = []
            for a in d.actuators:
                acts.append(f"{a.actuator_type}(idx={a.index})")
            log.info(f"  [{i}] {d.name} — actuators: {', '.join(acts) if acts else 'none'}")

        # Auto-map motors to device actuators
        self._auto_map_motors()
        return self.devices

    def _auto_map_motors(self):
        """Map KissToy motor IDs to Buttplug device/actuator indices."""
        self.motor_map = {}

        for motor_id, cfg in MOTOR_CONFIG.items():
            desired_type = cfg["actuator_type"]
            custom = self.custom_mapping.get(motor_id)

            if custom is not None and custom < len(self.devices):
                dev = self.devices[custom]
                # Find actuator of matching type on this device
                for act in dev.actuators:
                    if act.actuator_type == desired_type or act.actuator_type in ("Vibrate", "Oscillate", "Rotate"):
                        self.motor_map[motor_id] = {"device_index": custom, "actuator_index": act.index}
                        log.info(f"  M{motor_id} ({cfg['label']}) → device[{custom}] {dev.name}.{act.actuator_type}[{act.index}]")
                        break
                else:
                    if dev.actuators:
                        act = dev.actuators[0]
                        self.motor_map[motor_id] = {"device_index": custom, "actuator_index": act.index}
                        log.info(f"  M{motor_id} ({cfg['label']}) → device[{custom}] {dev.name}.{act.actuator_type}[{act.index}] (fallback)")
            else:
                # Auto: search all devices for matching actuator type
                for di, dev in enumerate(self.devices):
                    for act in dev.actuators:
                        if desired_type in act.actuator_type or act.actuator_type in ("Vibrate", "Oscillate", "Rotate"):
                            if motor_id not in self.motor_map:
                                self.motor_map[motor_id] = {"device_index": di, "actuator_index": act.index}
                                log.info(f"  M{motor_id} ({cfg['label']}) → device[{di}] {dev.name}.{act.actuator_type}[{act.index}]")
                                break

        # If still no mapping, fallback to first actuator of first device
        if not self.motor_map and self.devices:
            dev = self.devices[0]
            if dev.actuators:
                for motor_id in MOTOR_CONFIG:
                    if motor_id not in self.motor_map:
                        self.motor_map[motor_id] = {"device_index": 0, "actuator_index": dev.actuators[0].index}
                log.info(f"  All motors → device[0] {dev.name} (fallback)")

    async def send_motor_command(self, motors: dict):
        """Execute motor commands via Buttplug ScalarCmd."""
        if not self.client or not self.devices:
            log.warning("No device connected")
            return

        for motor_id, raw_value in motors.items():
            mapping = self.motor_map.get(motor_id)
            if not mapping:
                continue

            cfg = MOTOR_CONFIG.get(motor_id, {"max": 15, "actuator_type": "Vibrate"})
            intensity = max(0.0, min(1.0, int(raw_value) / cfg["max"]))

            try:
                cmd = ScalarCmd(
                    device_index=mapping["device_index"],
                    scalars=[Scalar(
                        index=mapping["actuator_index"],
                        scalar=intensity,
                        actuator_type=cfg["actuator_type"],
                    )],
                )
                await self.client.send(cmd)
                log.info(f"  M{motor_id}: {intensity:.2f} ({raw_value}/{cfg['max']})")
            except Exception as e:
                log.error(f"  M{motor_id} failed: {e}")

    async def stop_all(self):
        """Stop all motors."""
        if not self.client:
            return
        try:
            await self.client.stop_all()
            log.info("  All motors stopped")
        except Exception as e:
            log.error(f"Stop failed: {e}")

    async def run(self):
        """Main loop."""
        ws_url = f"{self.server_url}/ws/relay?group={self.group}&token={self.token}"
        log.info("=" * 50)
        log.info("polly-bridge Relay Client")
        log.info(f"  Server:  {self.server_url}")
        log.info(f"  Group:   {self.group[:16]}...")
        log.info(f"  Intiface: {self.intiface_url}")
        log.info("=" * 50)

        # Connect to Intiface
        while True:
            try:
                await self.connect_intiface()
                await self.scan_devices()
                break
            except Exception as e:
                log.warning(f"Intiface not available ({e}). Retrying in 5s...")
                await asyncio.sleep(5)

        # Connect to polly-bridge
        while True:
            try:
                async with websockets.connect(ws_url, ping_interval=20, ping_timeout=10) as ws:
                    log.info("✓ Connected to polly-bridge server")

                    # Send device info
                    device_list = []
                    for i, d in enumerate(self.devices):
                        acts = [{"index": a.index, "type": a.actuator_type} for a in d.actuators]
                        device_list.append({"index": i, "name": d.name, "actuators": acts})

                    await ws.send(json.dumps({
                        "type": "device_info",
                        "group": self.group,
                        "devices": device_list,
                        "motor_map": {m: {"device_index": v["device_index"], "actuator_index": v["actuator_index"]} for m, v in self.motor_map.items()},
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
                                await self.send_motor_command(motors)

                            # Send status back
                            await ws.send(json.dumps({
                                "type": "status",
                                "group": self.group,
                                "motors": motors,
                                "online": True,
                                "timestamp": time.time(),
                            }))

                        elif msg_type == "controller_online":
                            log.info("  🌐 Browser controller connected")

                        elif msg_type == "controller_offline":
                            log.info("  🌐 Browser controller disconnected — stopping")
                            await self.stop_all()

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
  python relay_client.py --server ws://localhost:8000 --group test --token dev
        """,
    )
    parser.add_argument("--server", required=True, help="polly-bridge server URL (wss://...)")
    parser.add_argument("--group", required=True, help="Group hash (from KissToy share link)")
    parser.add_argument("--token", required=True, help="RELAY_SECRET from Render env vars")
    parser.add_argument("--intiface", default="ws://127.0.0.1:12345", help="Intiface WebSocket URL")
    parser.add_argument("--motor-map", default=None, help='Motor→Device mapping JSON, e.g. \'{"1":0,"3":0}\'')
    args = parser.parse_args()

    device_mapping = None
    if args.motor_map:
        device_mapping = json.loads(args.motor_map)

    client = RelayClient(
        server_url=args.server,
        group=args.group,
        token=args.token,
        intiface_url=args.intiface,
        motor_device_map=device_mapping,
    )

    try:
        asyncio.run(client.run())
    except KeyboardInterrupt:
        log.info("Shutting down...")
        asyncio.run(client.stop_all())
        sys.exit(0)


if __name__ == "__main__":
    main()
