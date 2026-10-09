"""Quick, targeted test of the minimal working order recipe on a single
connection. Waits for auth completion before sending any setup steps,
then places ONE order per recipe variant.

Usage: .venv/bin/python -m debug.quick_recipe_test [--amount 10000] [--asset EURCAD_otc]
"""

from __future__ import annotations

import argparse
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
    build_order_message,
    build_tick_message,
    sanitize_asset,
)

SSID = os.environ.get("QSB_DEMO_SSID") or os.environ.get("QSB_SSID") or ""


async def run_one(ssid: str, asset: str, amount: int, steps: list[str], label: str) -> str:
    """Open one WS, wait for auth, run the given steps before the order, return result string."""
    ack = None
    depth_event = asyncio.Event()
    balance_val = None
    pending_header = None
    auth_sent = False
    auth_ready = asyncio.Event()

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
            nonlocal pending_header, auth_sent, balance_val, ack
            while True:
                try:
                    msg = await ws.recv()
                except websockets.ConnectionClosedOK:
                    return
                except websockets.ConnectionClosed:
                    return
                text = msg.decode("utf-8", "replace") if isinstance(msg, bytes) else msg

                # Engine.IO: server open handshake
                if text.startswith("0") and '"sid"' in text:
                    await ws.send("40")
                    continue
                # Engine.IO ping → pong
                if text == "2":
                    await ws.send("3")
                    continue
                # Socket.IO connect → send authorization
                if text.startswith("40") and not auth_sent:
                    auth_sent = True
                    await ws.send(build_authorization_message(ssid, True))
                    continue
                # Binary header (451-["event",{...}])
                if text.startswith("451-["):
                    pending_header = text.split('451-["', 1)[1].split('"', 1)[0]
                    if pending_header == "depth/change":
                        depth_event.set()
                    # instruments/list as binary event = auth confirmed
                    if pending_header in ("instruments/list", "s_authorization"):
                        auth_ready.set()
                    continue
                # Binary payload
                if text.startswith("\x04"):
                    try:
                        payload = json.loads(text[1:])
                    except Exception:
                        pending_header = None
                        continue
                    if pending_header in ("s_balance", "s_balance/list"):
                        for key in ("demoBalance", "balance", "amount", "value"):
                            val = payload.get(key)
                            if isinstance(val, (int, float)):
                                balance_val = val
                                break
                    if pending_header == "s_orders/open":
                        ack = json.dumps(payload)[:300]
                    pending_header = None
                    continue
                # Text event
                if text.startswith("42["):
                    try:
                        parts = json.loads(text[2:])
                        name, args = parts[0], parts[1:]
                    except Exception:
                        continue
                    # Auth confirmed by instruments/list push
                    if name == "instruments/list":
                        auth_ready.set()
                    if name == "s_authorization":
                        auth_ready.set()
                    if name in ("successupdateBalance",) and args:
                        if isinstance(args[0], dict):
                            for key in ("demoBalance", "balance", "amount", "value"):
                                val = args[0].get(key)
                                if isinstance(val, (int, float)):
                                    balance_val = val
                                    break
                    if name in ("s_orders/open", "successopenOrder") and args:
                        if isinstance(args[0], dict):
                            ack = json.dumps(args[0])[:300]
                    if name == "error" and args:
                        print(f"  [{label}] SERVER ERROR: {json.dumps(args)[:200]}", flush=True)
                    continue

        rt = asyncio.create_task(reader())

        # Wait for auth handshake to complete (instruments/list confirms success)
        try:
            await asyncio.wait_for(auth_ready.wait(), timeout=10)
        except asyncio.TimeoutError:
            rt.cancel()
            return "❌ AUTH TIMEOUT"

        # Request balance
        await ws.send('42["balance/list"]')
        await asyncio.sleep(2)
        if balance_val is not None:
            print(f"  [{label}] balance: {balance_val}", flush=True)
        else:
            print(f"  [{label}] balance: (not received)", flush=True)

        # Run setup steps
        for step in steps:
            await ws.send(step)
            await asyncio.sleep(1.0)

        # Wait for depth if needed
        if any("depth/follow" in s for s in steps):
            try:
                await asyncio.wait_for(depth_event.wait(), timeout=6)
                print(f"  [{label}] depth/change received", flush=True)
            except asyncio.TimeoutError:
                print(f"  [{label}] depth/change TIMEOUT (proceeding anyway)", flush=True)

        # Place order
        rid = int(time.time())
        order = build_order_message(
            asset=asset, amount=amount, time_field=60, action="put",
            is_demo=True, request_id=rid, option_type=100,
        )
        await ws.send(order)
        await asyncio.sleep(8)

        rt.cancel()

    if ack:
        return f"✅ ACK: {ack}"
    return "❌ DROPPED"


async def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--amount", type=int, default=10000)
    parser.add_argument("--asset", type=str, default="EURCAD_otc")
    args = parser.parse_args()

    asset = sanitize_asset(args.asset)
    amount = args.amount

    if not SSID:
        print("Set QSB_DEMO_SSID or QSB_SSID", flush=True)
        return

    print(f"Token: {SSID[:8]}...{SSID[-4:]} (len={len(SSID)})")
    print(f"Asset: {asset}, Amount: {amount}\n")

    from broker.protocol import build_follow_message

    # Recipe 1: Bot's current (known broken — for comparison)
    print("--- Recipe A: bot current (tick + instruments/follow + order) ---")
    result_a = await run_one(
        SSID, asset, amount,
        steps=[build_tick_message(), build_follow_message(asset)],
        label="A",
    )
    print(f"  Result: {result_a}\n")

    await asyncio.sleep(2)

    # Recipe 2: Minimal fix hypothesis (tick + instruments/update + depth/follow + order)
    print("--- Recipe B: minimal fix (tick + instruments/update + depth/follow + order) ---")
    result_b = await run_one(
        SSID, asset, amount,
        steps=[
            build_tick_message(),
            f'42["instruments/update",{{"asset":"{asset}","period":60}}]',
            f'42["depth/follow","{asset}"]',
        ],
        label="B",
    )
    print(f"  Result: {result_b}\n")

    await asyncio.sleep(2)

    # Recipe 3: Just depth/follow (no instruments/update)
    print("--- Recipe C: depth-only (tick + depth/follow + order) ---")
    result_c = await run_one(
        SSID, asset, amount,
        steps=[build_tick_message(), f'42["depth/follow","{asset}"]'],
        label="C",
    )
    print(f"  Result: {result_c}\n")

    # Summary
    print("=" * 60)
    print(f"A (bot current):         {result_a}")
    print(f"B (inst_update + depth): {result_b}")
    print(f"C (depth only):          {result_c}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
