"""Bisect which setup messages + timing make the server accept an order.

Each variant sends a prefix then the order, spaced (1s) or in a burst.
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

FULL = [
    '42["pending/list"]',
    '42["balance/list"]',
    '42["indicator/list"]',
    '42["drawing/load"]',
    '42["instruments/update",{"asset":"EURCAD_otc","period":60}]',
    '42["chart_notification/get",{"asset":"EURCAD_otc","version":"1.0.0"}]',
    '42["depth/follow","EURCAD_otc"]',
]
MIN = [
    '42["instruments/update",{"asset":"EURCAD_otc","period":60}]',
    '42["depth/follow","EURCAD_otc"]',
]
MIN2 = [
    '42["pending/list"]',
    '42["balance/list"]',
    '42["instruments/update",{"asset":"EURCAD_otc","period":60}]',
    '42["depth/follow","EURCAD_otc"]',
]

# label -> (prefix msgs, spaced_bool)
VARIANTS = [
    ("FULL spaced", FULL, True),
    ("FULL burst", FULL, False),
    ("MIN spaced", MIN, True),
    ("MIN burst", MIN, False),
    ("MIN2 spaced", MIN2, True),
]


async def main() -> None:
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
                    pending_header = text.split("451-[", 1)[1].lstrip('"')
                    continue
                if text.startswith("\x04"):
                    try:
                        payload = json.loads(text[1:])
                    except Exception:
                        continue
                    if pending_header == "s_orders/open":
                        acks.append(str(payload.get("requestId")))
                    pending_header = None
                    continue

        rt = asyncio.create_task(reader())
        await asyncio.sleep(3)

        for i, (label, prefix, spaced) in enumerate(VARIANTS):
            acks.clear()
            rid = int(time.time()) + i
            await ws.send(build_tick_message())
            for m in prefix:
                await ws.send(m)
                if spaced:
                    await asyncio.sleep(1.0)
            frame = ('42["orders/open",{"asset":"EURCAD_otc","amount":10000,"time":60,'
                     f'"action":"put","isDemo":1,"tournamentId":0,"requestId":{rid},"optionType":100}}]')
            await ws.send(frame)
            await asyncio.sleep(6)
            ok = str(rid) in acks
            print(f"[{label}] -> {'ACK' if ok else 'DROPPED'}", flush=True)

        rt.cancel()


if __name__ == "__main__":
    asyncio.run(main())
