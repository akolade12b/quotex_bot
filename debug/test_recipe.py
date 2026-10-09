"""Verify the complete working recipe on a raw WS connection:

  tick -> instruments/update -> depth/follow -> wait for depth/change -> order
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

import websockets

from broker.protocol import (
    SOCKETIO_ENDPOINT,
    build_authorization_message,
    build_tick_message,
)

SSID = os.environ.get("QSB_DEMO_SSID") or os.environ.get("QSB_SSID") or ""
WS_URL = os.environ.get("QSB_WS_URL") or SOCKETIO_ENDPOINT
ASSET = "EURCAD_otc"


async def main() -> None:
    depth_ready = asyncio.Event()
    acks: list[str] = []

    async with websockets.connect(
        WS_URL,
        origin="https://quotex.com",
        user_agent_header=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ),
        additional_headers={"Referer": "https://quotex.com/", "Accept-Language": "en-US,en;q=0.9"},
        ping_interval=None,
    ) as ws:
        auth_sent = False
        pending_header = None

        async def reader() -> None:
            nonlocal auth_sent, pending_header
            while True:
                msg = await ws.recv()
                text = msg.decode("utf-8", "replace") if isinstance(msg, bytes) else msg
                if text.startswith("0") and '"sid"' in text:
                    await ws.send("40")
                    continue
                if text == "2":
                    await ws.send("3")
                    continue
                if text.startswith("40") and not auth_sent:
                    auth_sent = True
                    await ws.send(build_authorization_message(SSID, True))
                    continue
                if text.startswith("451-["):
                    pending_header = text.split('451-["', 1)[1].split('"', 1)[0]
                    if pending_header == "depth/change":
                        depth_ready.set()
                    continue
                if text.startswith("\x04"):
                    try:
                        payload = json.loads(text[1:])
                    except Exception:
                        continue
                    if pending_header == "s_orders/open":
                        acks.append(json.dumps(payload)[:200])
                    pending_header = None
                    continue

        rt = asyncio.create_task(reader())
        await asyncio.sleep(3)  # auth + initial pushes

        await ws.send(build_tick_message())
        await asyncio.sleep(0.5)
        await ws.send(f'42["instruments/update",{{"asset":"{ASSET}","period":60}}]')
        await asyncio.sleep(0.5)
        await ws.send(f'42["depth/follow","{ASSET}"]')
        print("waiting for depth/change…", flush=True)
        try:
            await asyncio.wait_for(depth_ready.wait(), timeout=10)
            print("depth subscription ACTIVE", flush=True)
        except asyncio.TimeoutError:
            print("depth NOT active — aborting", flush=True)
            rt.cancel()
            return

        rid = int(time.time())
        frame = ('42["orders/open",{"asset":"EURCAD_otc","amount":10000,"time":60,'
                 f'"action":"put","isDemo":1,"tournamentId":0,"requestId":{rid},"optionType":100}}]')
        print("order:", frame, flush=True)
        await ws.send(frame)
        await asyncio.sleep(10)
        print("ACKS:", acks if acks else "DROPPED", flush=True)
        rt.cancel()


if __name__ == "__main__":
    asyncio.run(main())
