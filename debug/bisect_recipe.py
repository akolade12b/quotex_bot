"""Bisect recipe D down to the minimal set of setup messages that makes the
server accept an order. Each recipe runs on its own connection.

Usage: QSB_DEMO_SSID=<fresh> .venv/bin/python -m debug.bisect_recipe
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
ASSET = "EURCAD_otc"

PENDING = '42["pending/list"]'
BALANCE = '42["balance/list"]'
INDICATOR = '42["indicator/list"]'
DRAWING = '42["drawing/load"]'
INST_UPDATE = f'42["instruments/update",{{"asset":"{ASSET}","period":60}}]'
CHART_NOTIF = '42["chart_notification/get",{"asset":"EURCAD_otc","version":"1.0.0"}]'
DEPTH = f'42["depth/follow","{ASSET}"]'

# label -> setup messages (tick + depth-follow added automatically, depth-wait applied)
VARIANTS = [
    ("M1: inst_update+depth", [INST_UPDATE, DEPTH]),
    ("M2: +balance", [BALANCE, INST_UPDATE, DEPTH]),
    ("M3: +pending+indicator+drawing", [PENDING, BALANCE, INDICATOR, DRAWING, INST_UPDATE, DEPTH]),
    ("M4: full (=D)", [PENDING, BALANCE, INDICATOR, DRAWING, INST_UPDATE, CHART_NOTIF, DEPTH]),
    ("M5: balance+depth only", [BALANCE, DEPTH]),
]


async def one(ssid: str, label: str, setup: list[str]) -> str:
    depth_ready = asyncio.Event()
    ack: list[str] = []
    pending_header = None
    auth_sent = False

    async with websockets.connect(
        SOCKETIO_ENDPOINT,
        origin="https://quotex.com",
        user_agent_header=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ),
        additional_headers={"Referer": "https://quotex.com/", "Accept-Language": "en-US,en;q=0.9"},
        ping_interval=None,
    ) as ws:
        async def reader() -> None:
            nonlocal pending_header, auth_sent
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
                    await ws.send(build_authorization_message(ssid, True))
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
                        ack.append(payload.get("id"))
                    pending_header = None
                    continue

        rt = asyncio.create_task(reader())
        await asyncio.sleep(3)
        await ws.send(build_tick_message())
        for m in setup:
            await ws.send(m)
            await asyncio.sleep(1.0)
        try:
            await asyncio.wait_for(depth_ready.wait(), timeout=8)
            depth_ok = True
        except asyncio.TimeoutError:
            depth_ok = False
        rid = int(time.time())
        frame = ('42["orders/open",{"asset":"EURCAD_otc","amount":10000,"time":60,'
                 f'"action":"put","isDemo":1,"tournamentId":0,"requestId":{rid},"optionType":100}}]')
        await ws.send(frame)
        await asyncio.sleep(8)
        rt.cancel()
    return f"depth={'Y' if depth_ok else 'N'} ack={'Y' if ack else 'N'}"


async def main() -> None:
    if not SSID:
        print("set QSB_DEMO_SSID to a fresh token", flush=True)
        return
    for label, setup in VARIANTS:
        r = await one(SSID, label, setup)
        print(f"[{label}] -> {r}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
