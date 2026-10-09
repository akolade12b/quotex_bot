"""Ground truth: place the bot's exact order over raw WS, then read the balance
on the same connection before/after. Catches acks via BOTH binary (451) and
text (42) events."""

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
ASSET = "EURCAD_otc"


async def main() -> None:
    balance = {}
    events: list[str] = []
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
                    await ws.send(build_authorization_message(SSID, True))
                    continue
                if text.startswith("451-["):
                    pending_header = text.split('451-["', 1)[1].split('"', 1)[0]
                    continue
                if text.startswith("\x04"):
                    try:
                        payload = json.loads(text[1:])
                    except Exception:
                        pending_header = None
                        continue
                    if pending_header in ("s_balance", "s_balance/list"):
                        balance["s_balance"] = payload
                    if pending_header == "s_orders/open":
                        events.append("BINARY:" + json.dumps(payload)[:200])
                    pending_header = None
                    continue
                if text.startswith("42["):
                    try:
                        name, args = json.loads(text[2:])[0], json.loads(text[2:])[1:]
                    except Exception:
                        continue
                    if name in ("s_orders/open", "successopenOrder", "error", "successupdateBalance"):
                        events.append(f"TEXT:{name}:{json.dumps(args)[:200]}")
                    if name == "successupdateBalance":
                        if args and isinstance(args[0], dict):
                            balance["s_balance"] = args[0]

        rt = asyncio.create_task(reader())
        await asyncio.sleep(3)

        # read starting balance (real via balance/list)
        await ws.send('42["balance/list"]')
        await asyncio.sleep(3)
        print("balance before:", json.dumps(balance.get("s_balance"))[:150], flush=True)

        rid = int(time.time())
        frame = build_order_message(
            asset=ASSET, amount=10000, time_field=60, action="put",
            is_demo=True, request_id=rid, option_type=100,
        )
        print("order:", frame, flush=True)
        await ws.send(build_tick_message())
        await ws.send(build_follow_message(ASSET))
        await ws.send(frame)
        await asyncio.sleep(12)

        await ws.send('42["balance/list"]')
        await asyncio.sleep(3)
        print("balance after:", json.dumps(balance.get("s_balance"))[:150], flush=True)
        print("events:", json.dumps(events), flush=True)
        rt.cancel()


if __name__ == "__main__":
    asyncio.run(main())
