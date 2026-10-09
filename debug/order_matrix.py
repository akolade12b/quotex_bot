"""Try several orders/open payload shapes against the live demo server and
report which one the server acknowledges (s_orders/open) vs silently drops.

Each acknowledged order is a real demo trade (small amounts). This pinpoints
the exact field set + amount the platform currently accepts.
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
    build_balance_request,
    build_tick_message,
    sanitize_asset,
)

SSID = os.environ.get("QSB_DEMO_SSID") or os.environ.get("QSB_SSID") or ""
WS_URL = os.environ.get("QSB_WS_URL") or SOCKETIO_ENDPOINT
IS_DEMO = True

# A probe: (label, asset, amount, time_field, action, optionType, extra_payload)
PROBES = [
    ("OTC amount=10000 t=60", "EURCAD_otc", 10000, 60, "put", 100, None),
    ("OTC amount=10000 t=300", "EURCAD_otc", 10000, 300, "put", 100, None),
    ("OTC amount=1000 t=60", "EURCAD_otc", 1000, 60, "put", 100, None),
    ("OTC amount=100 t=60", "EURCAD_otc", 100, 60, "put", 100, None),
    ("REG amount=10000 t=epoch", "EURCAD", 10000, 0, "put", 1, None),  # time filled at runtime
    ("REG amount=10000 t=60", "EURCAD", 10000, 60, "put", 1, None),
    ("OTC amount=10000 t=60 +exp", "EURCAD_otc", 10000, 60, "put", 100,
     {"exp": 60, "expirationTime": 0}),
]


def _epoch_time_for_minutes(minutes: int = 1) -> int:
    import datetime
    now = datetime.datetime.now()
    shift = 1 if now.second >= 30 else 0
    base = now.replace(second=0, microsecond=0) + datetime.timedelta(minutes=shift + minutes)
    return int(base.timestamp())


async def main() -> None:
    if not SSID:
        print("no ssid", flush=True)
        return

    acks: dict[int, dict] = {}
    req_labels: dict[int, str] = {}

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
        authorized = asyncio.Event()

        async def reader() -> None:
            nonlocal auth_sent
            pending_header = None
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
                    continue
                if text.startswith("\x04"):
                    try:
                        payload = json.loads(text[1:])
                    except Exception:
                        continue
                    if pending_header == "s_orders/open":
                        rid = str(payload.get("requestId"))
                        acks[int(rid) if rid else -1] = payload
                        print(f"  !! ACK requestId={rid} id={payload.get('id')} "
                              f"openPrice={payload.get('openPrice')}", flush=True)
                        authorized.set()
                    pending_header = None
                    continue
                if text.startswith("42"):
                    if "s_authorization" in text or "instruments/list" in text:
                        authorized.set()

        rt = asyncio.create_task(reader())

        try:
            await asyncio.wait_for(authorized.wait(), timeout=20)
        except asyncio.TimeoutError:
            print("no auth", flush=True)
            return

        await ws.send(build_balance_request())
        await ws.send(build_tick_message())

        for i, (label, asset, amount, t, action, otype, extra) in enumerate(PROBES):
            req_id = int(time.time() * 1000) + i
            asset = sanitize_asset(asset)
            if t == 0:
                t = _epoch_time_for_minutes(1)
            payload = {
                "asset": asset,
                "amount": amount,
                "time": t,
                "action": action,
                "isDemo": 1 if IS_DEMO else 0,
                "tournamentId": 0,
                "requestId": req_id,
                "optionType": otype,
            }
            if extra:
                payload.update(extra)
            frame = '42["orders/open",' + json.dumps(payload, separators=(",", ":")) + "]"
            req_labels[req_id] = label
            print(f"[{label}] <<OUT {frame}", flush=True)
            await ws.send(frame)
            await asyncio.sleep(3)
            if req_id not in acks:
                print(f"[{label}] -> no ack after 3s", flush=True)

        # wait a bit more to catch late acks
        await asyncio.sleep(10)
        print("\n=== FINAL ===", flush=True)
        for rid in sorted(acks):
            a = acks[rid]
            print(f"  ACK [{req_labels.get(rid, '?')}] rid={rid} id={a.get('id')} "
                  f"openPrice={a.get('openPrice')} asset={a.get('asset')} amount={a.get('amount')}",
                  flush=True)
        print("(only ACKed probes are listed above — unlisted probes were dropped)", flush=True)

        rt.cancel()


if __name__ == "__main__":
    asyncio.run(main())
