"""Raw Quotex WebSocket frame dumper.

Connects with the demo SSID from .env, authenticates, dumps EVERY inbound and
outbound frame verbatim to stdout, then (optionally) sends an orders/open and
watches for the server's response. Use this to see exactly what the platform
says when an order is rejected silently.

Usage:
    .venv/bin/python -m debug.dump_frames            # handshake + idle dump
    .venv/bin/python -m debug.dump_frames --order    # also place a demo order
    .venv/bin/python -m debug.dump_frames --order --asset EURCAD_otc --amount 10 --action put --expiry 60
"""

from __future__ import annotations

import argparse
import asyncio
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
    build_follow_message,
    build_order_message,
    build_tick_message,
    compute_order_time_and_type,
    sanitize_asset,
)

SSID = os.environ.get("QSB_DEMO_SSID") or os.environ.get("QSB_SSID") or ""
WS_URL = os.environ.get("QSB_WS_URL") or SOCKETIO_ENDPOINT
IS_DEMO = os.environ.get("QSB_QUOTEX_MODE", "demo") != "live"


def _show(raw: bytes | str) -> str:
    if isinstance(raw, bytes):
        text = raw.decode("utf-8", "replace")
    else:
        text = raw
    return text if len(text) <= 1000 else text[:1000] + f"...<{len(text)} bytes>"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--order", action="store_true")
    parser.add_argument("--asset", default="EURCAD")
    parser.add_argument("--amount", type=float, default=10.0)
    parser.add_argument("--action", default="put")
    parser.add_argument("--expiry", type=int, default=60)
    parser.add_argument("--seconds", type=int, default=0, help="if >0, use optionType 100 (seconds)")
    args = parser.parse_args()

    if not SSID:
        print("no QSB_DEMO_SSID / QSB_SSID in .env — set one first", flush=True)
        return

    print(f"session: {SSID[:6]}…{SSID[-4:]}  demo={IS_DEMO}  ws={WS_URL}", flush=True)

    async with websockets.connect(
        WS_URL,
        origin="https://quotex.com",
        user_agent_header=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ),
        additional_headers={
            "Referer": "https://quotex.com/",
            "Accept-Language": "en-US,en;q=0.9",
        },
        ping_interval=None,
    ) as ws:
        auth_sent = False
        authorized = asyncio.Event()
        errors: list[str] = []

        async def reader() -> None:
            nonlocal auth_sent
            try:
                while True:
                    try:
                        msg = await ws.recv()
                    except websockets.ConnectionClosed as exc:
                        print(f"[{time.strftime('%H:%M:%S')}] !! connection closed: {exc}", flush=True)
                        return
                    print(f"[{time.strftime('%H:%M:%S')}] >>IN {_show(msg)}", flush=True)
                    text = msg.decode("utf-8", "replace") if isinstance(msg, bytes) else msg

                    if text.startswith("0") and '"sid"' in text:
                        print("    -> Engine.IO open; sending '40'", flush=True)
                        await ws.send("40")
                        continue
                    if text == "2":
                        await ws.send("3")
                        continue
                    if text.startswith("40") and not auth_sent:
                        auth_sent = True
                        frame = build_authorization_message(SSID, IS_DEMO)
                        print(f"[{time.strftime('%H:%M:%S')}] <<OUT {_show(frame)}", flush=True)
                        await ws.send(frame)
                        continue
                    if text.startswith("42") and ("s_authorization" in text or "instruments/list" in text):
                        authorized.set()
            except Exception as exc:
                errors.append(repr(exc))
                print(f"!! reader task died: {exc!r}", flush=True)
                raise

        reader_task = asyncio.create_task(reader())

        try:
            await asyncio.wait_for(authorized.wait(), timeout=20)
            print("  -> authorized OK", flush=True)
        except asyncio.TimeoutError:
            print("  !! never got s_authorization", flush=True)

        if not args.order:
            print("  -> idle-dumping 12s (no order)…", flush=True)
            await asyncio.sleep(12)
        else:
            await ws.send(build_balance_request())
            await ws.send(build_tick_message())
            asset = sanitize_asset(args.asset)
            await ws.send(build_follow_message(asset))

            req_id = int(time.time())  # epoch SECONDS — matches the website
            if args.seconds:
                option_type, time_field = 100, args.expiry
            else:
                option_type, time_field = compute_order_time_and_type(asset, args.expiry, time.time())

            frame = build_order_message(
                asset=asset,
                amount=args.amount,
                time_field=time_field,
                action=args.action,
                is_demo=IS_DEMO,
                request_id=req_id,
                option_type=option_type,
            )
            print(f"\n[{time.strftime('%H:%M:%S')}] <<OUT {_show(frame)}", flush=True)
            await ws.send(frame)
            print(f"[{time.strftime('%H:%M:%S')}]  -> sent orders/open, watching 25s for response…", flush=True)
            await asyncio.sleep(25)

        reader_task.cancel()
        try:
            await reader_task
        except asyncio.CancelledError:
            pass
        if errors:
            print("errors:", errors, flush=True)


if __name__ == "__main__":
    asyncio.run(main())
