"""Isolate the minimal fix: does sending depth/follow before the order (in a
rapid burst, like the bot does) make the server accept it?"""

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
    build_follow_message,
    build_order_message,
    build_tick_message,
)

SSID = os.environ.get("QSB_DEMO_SSID") or os.environ.get("QSB_SSID") or ""
WS_URL = os.environ.get("QSB_WS_URL") or SOCKETIO_ENDPOINT
ASSET = "EURCAD_otc"

# variants: (label, include_depth_follow, include_instruments_follow, wait_after_setup_s)
VARIANTS = [
    ("tick+follow+order (bot current)", False, True, 0.0),
    ("tick+follow+depth+order (burst)", True, True, 0.0),
    ("tick+follow+depth+order (wait1s)", True, True, 1.0),
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
        depth_seen = False

        async def reader() -> None:
            nonlocal auth_sent, pending_header, depth_seen
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
                    if pending_header == "depth/change":
                        depth_seen = True
                    continue
                if text.startswith("\x04"):
                    try:
                        payload = json.loads(text[1:])
                    except Exception:
                        continue
                    if pending_header == "s_orders/open":
                        acks.append(f"id={payload.get('id')} price={payload.get('openPrice')}")
                    pending_header = None
                    continue

        rt = asyncio.create_task(reader())
        await asyncio.sleep(3)  # auth + initial pushes settle

        for i, (label, use_depth, use_follow, wait_s) in enumerate(VARIANTS):
            depth_seen = False
            acks.clear()
            rid = int(time.time()) + i
            msgs = [build_tick_message()]
            if use_follow:
                msgs.append(build_follow_message(ASSET))
            if use_depth:
                msgs.append(f'42["depth/follow","{ASSET}"]')
            for m in msgs:
                await ws.send(m)
            if wait_s:
                await asyncio.sleep(wait_s)
            option_type, time_field = 100, 60
            frame = build_order_message(
                asset=ASSET, amount=10000, time_field=time_field, action="put",
                is_demo=True, request_id=rid, option_type=option_type,
            )
            await ws.send(frame)
            await asyncio.sleep(6)
            print(f"[{label}] depth_seen={depth_seen} ack={acks if acks else 'DROPPED'}", flush=True)

        rt.cancel()


if __name__ == "__main__":
    asyncio.run(main())
