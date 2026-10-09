"""Connect and dump the instruments/list entries for EURCAD (regular + OTC)
so we can read allowed expiries, directions, and the order payload shape."""

from __future__ import annotations

import asyncio
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

import websockets

from broker.protocol import SOCKETIO_ENDPOINT, build_authorization_message, parse_binary_header

SSID = os.environ.get("QSB_DEMO_SSID") or os.environ.get("QSB_SSID") or ""
WS_URL = os.environ.get("QSB_WS_URL") or SOCKETIO_ENDPOINT
IS_DEMO = os.environ.get("QSB_QUOTEX_MODE", "demo") != "live"


async def main() -> None:
    if not SSID:
        print("no ssid", flush=True)
        return
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
        pending_header: str | None = None
        instruments: list | None = None

        async def reader() -> None:
            nonlocal auth_sent, pending_header, instruments
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
                    await ws.send(build_authorization_message(SSID, IS_DEMO))
                    continue
                if text.startswith("451-["):
                    pending_header = parse_binary_header(text)
                    continue
                if text.startswith("\x04") and pending_header == "instruments/list":
                    instruments = json.loads(text[1:])
                    return

        t = asyncio.create_task(reader())
        try:
            await asyncio.wait_for(t, timeout=20)
        except asyncio.TimeoutError:
            print("no instruments captured", flush=True)
            return

        if instruments is None:
            print("captured none", flush=True)
            return
        print(f"captured {len(instruments)} instruments", flush=True)
        for row in instruments:
            sym = row[1] if isinstance(row, list) and len(row) > 1 else ""
            if "EURCAD" in str(sym).upper():
                print(json.dumps(row, ensure_ascii=False), flush=True)


if __name__ == "__main__":
    asyncio.run(main())
