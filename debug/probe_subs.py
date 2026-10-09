"""Probe which subscriptions the raw WS connection can activate, and whether
the browser's exact (spaced-out) setup changes server behavior. Records every
inbound frame's event name so we can see what the server actually pushes."""

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
    sanitize_asset,
)

SSID = os.environ.get("QSB_DEMO_SSID") or os.environ.get("QSB_SSID") or ""
WS_URL = os.environ.get("QSB_WS_URL") or SOCKETIO_ENDPOINT

seen: list[str] = []


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
                    if pending_header not in seen:
                        seen.append(pending_header)
                        print(f"  event: {pending_header}", flush=True)
                    continue
                if text.startswith("\x04"):
                    if pending_header == "s_orders/open":
                        try:
                            payload = json.loads(text[1:])
                            print(f"  !! ORDER ACK id={payload.get('id')} "
                                  f"price={payload.get('openPrice')} bal={payload.get('accountBalance')}",
                                  flush=True)
                        except Exception:
                            pass
                    pending_header = None
                    continue
                if text.startswith("42["):
                    try:
                        name = json.loads(text[2:])[0]
                    except Exception:
                        name = "?"
                    if name not in seen:
                        seen.append(name)
                        print(f"  event: {name}", flush=True)

        rt = asyncio.create_task(reader())
        await asyncio.sleep(3)  # auth + initial pushes

        asset = sanitize_asset("EURCAD_otc")
        print("\n-- sending browser-style spaced setup --", flush=True)
        await ws.send(build_tick_message())
        await asyncio.sleep(1)
        await ws.send('42["pending/list"]')
        await asyncio.sleep(1)
        await ws.send('42["balance/list"]')
        await asyncio.sleep(1)
        await ws.send('42["indicator/list"]')
        await asyncio.sleep(1)
        await ws.send('42["drawing/load"]')
        await asyncio.sleep(1)
        await ws.send('42["instruments/update",{"asset":"EURCAD_otc","period":60}]')
        await asyncio.sleep(1)
        await ws.send('42["chart_notification/get",{"asset":"EURCAD_otc","version":"1.0.0"}]')
        await asyncio.sleep(1)
        print("-- depth/follow --", flush=True)
        await ws.send(f'42["depth/follow","{asset}"]')
        await asyncio.sleep(8)
        print("-- instruments/follow --", flush=True)
        await ws.send(f'42["instruments/follow","{asset}"]')
        await asyncio.sleep(8)

        # now the order
        rid = int(time.time())
        frame = ('42["orders/open",{"asset":"EURCAD_otc","amount":10000,"time":60,'
                 f'"action":"put","isDemo":1,"tournamentId":0,"requestId":{rid},"optionType":100}}]')
        print(f"\n-- order: {frame}", flush=True)
        await ws.send(frame)
        await asyncio.sleep(12)

        print("\n=== distinct events seen ===", flush=True)
        for s in seen:
            print("  ", s, flush=True)
        rt.cancel()


if __name__ == "__main__":
    asyncio.run(main())
