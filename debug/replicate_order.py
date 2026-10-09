"""Replicate the browser's EXACT order sequence over raw WebSocket.

Mirrors the setup messages the real trade page sends after auth, then the
orders/open payload with integer amount + epoch-seconds requestId. Reports
whether the server acks (s_orders/open) or drops it silently.
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
    sanitize_asset,
)

SSID = os.environ.get("QSB_DEMO_SSID") or os.environ.get("QSB_SSID") or ""
WS_URL = os.environ.get("QSB_WS_URL") or SOCKETIO_ENDPOINT
IS_DEMO = True

# (label, asset, amount, time_s, action, option_type)
PROBES = [
    ("int amount, sec reqid", "EURCAD_otc", 10000, 60, "put", 100),
    ("float amount, sec reqid", "EURCAD_otc", 10000.0, 60, "put", 100),
    ("int amount, ms reqid", "EURCAD_otc", 10000, 60, "put", 100),
    ("int amount 10", "EURCAD_otc", 10, 60, "put", 100),
]

DEPTH_READY = asyncio.Event()


async def main() -> None:
    if not SSID:
        print("no ssid", flush=True)
        return

    acks: dict[int, dict] = {}
    req_label: dict[int, str] = {}

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
                    await ws.send(build_authorization_message(SSID, IS_DEMO))
                    continue
                if text.startswith("451-["):
                    pending_header = text.split("451-[", 1)[1].lstrip('"')
                    if pending_header == "depth/change":
                        DEPTH_READY.set()
                    continue
                if text.startswith("\x04"):
                    try:
                        payload = json.loads(text[1:])
                    except Exception:
                        continue
                    if pending_header == "depth/change":
                        DEPTH_READY.set()
                    if pending_header == "s_orders/open":
                        rid = payload.get("requestId")
                        acks[rid if isinstance(rid, int) else -1] = payload
                        print(f"  !! ACK requestId={rid} id={payload.get('id')} "
                              f"openPrice={payload.get('openPrice')} accountBalance={payload.get('accountBalance')}",
                              flush=True)
                    pending_header = None
                    continue

        rt = asyncio.create_task(reader())
        await asyncio.sleep(2)  # let auth settle
        asset = sanitize_asset("EURCAD_otc")

        for i, (label, _asset, amount, t, action, otype) in enumerate(PROBES):
            rid = int(time.time()) + i  # epoch seconds
            req_label[rid] = label
            # setup sequence the browser sends
            for m in (
                build_tick_message(),
                '42["pending/list"]',
                '42["balance/list"]',
                '42["indicator/list"]',
                '42["drawing/load"]',
                '42["instruments/update",{"asset":"EURCAD_otc","period":60}]',
                '42["chart_notification/get",{"asset":"EURCAD_otc","version":"1.0.0"}]',
                '42["depth/follow","EURCAD_otc"]',
            ):
                await ws.send(m)
            # wait for the depth subscription to actually become active
            DEPTH_READY.clear()
            try:
                await asyncio.wait_for(DEPTH_READY.wait(), timeout=5)
                print(f"[{label}] depth subscription active", flush=True)
            except asyncio.TimeoutError:
                print(f"[{label}] !! no depth/change — subscription may not be active", flush=True)
            await asyncio.sleep(1)
            payload = {
                "asset": asset,
                "amount": amount,
                "time": int(t),
                "action": action,
                "isDemo": 1 if IS_DEMO else 0,
                "tournamentId": 0,
                "requestId": rid,
                "optionType": otype,
            }
            frame = '42["orders/open",' + json.dumps(payload, separators=(",", ":")) + "]"
            print(f"\n[{label}] <<OUT {frame}", flush=True)
            await ws.send(frame)
            await asyncio.sleep(4)
            if rid not in acks:
                print(f"[{label}] -> DROPPED", flush=True)

        await asyncio.sleep(8)
        print("\n=== ACKS ===", flush=True)
        for rid in sorted(acks):
            a = acks[rid]
            print(f"  [{req_label.get(rid)}] rid={rid} id={a.get('id')} price={a.get('openPrice')}", flush=True)
        rt.cancel()


if __name__ == "__main__":
    asyncio.run(main())
