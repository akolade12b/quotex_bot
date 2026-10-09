"""Connect with a token and dump current balance + most recent closed orders +
open orders, to see which test trades actually went through."""

from __future__ import annotations

import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

import websockets

from broker.protocol import SOCKETIO_ENDPOINT, build_authorization_message

SSID = os.environ.get("QSB_DEMO_SSID") or os.environ.get("QSB_SSID") or ""
WS_URL = os.environ.get("QSB_WS_URL") or SOCKETIO_ENDPOINT


async def main() -> None:
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
        balance = None
        closed = []
        opened = []

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
                    print("header:", pending_header, flush=True)
                    continue
                if text.startswith("\x04"):
                    print("payload:", text[1:90], flush=True)
                    try:
                        payload = json.loads(text[1:])
                    except Exception as exc:
                        print("  parse fail:", exc, flush=True)
                        continue
                    if pending_header == "s_balance/list":
                        balance = payload
                    elif pending_header == "orders/closed/list":
                        closed = payload if isinstance(payload, list) else []
                    elif pending_header == "orders/opened/list":
                        opened = payload if isinstance(payload, list) else []
                    pending_header = None
                    continue

        rt = asyncio.create_task(reader())
        await asyncio.sleep(6)
        rt.cancel()

        print("balance:", json.dumps(balance)[:200], flush=True)
        print(f"open orders: {len(opened)}", flush=True)
        for o in opened:
            print("  OPEN", json.dumps(o)[:200], flush=True)
        print(f"closed orders (last 15):", flush=True)
        for c in closed[:15]:
            print("  ", c.get("openTime"), c.get("asset"), c.get("command"), "amount", c.get("amount"),
                  "profit", c.get("profit"), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
