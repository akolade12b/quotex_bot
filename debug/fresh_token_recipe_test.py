"""Get a fresh browser-minted token, then test 3 order recipes to find the
minimal working one. This is the definitive bisection experiment.

Recipe A: bot current (tick + instruments/follow + order) — expected DROPPED
Recipe B: minimal fix (tick + instruments/update + depth/follow + order)
Recipe C: depth-only (tick + depth/follow + order) — is instruments/update needed?

Usage: .venv/bin/python -m debug.fresh_token_recipe_test [--amount 10000] [--asset EURCAD_otc]
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

EMAIL = os.environ.get("QSB_EMAIL", "")
PASSWORD = os.environ.get("QSB_PASSWORD", "")

from broker.protocol import (
    SOCKETIO_ENDPOINT,
    DEMO_TRADE_PAGE_URL,
    LOGIN_PAGE_URL,
    build_authorization_message,
    build_follow_message,
    build_order_message,
    build_tick_message,
    sanitize_asset,
)


async def get_fresh_token() -> str | None:
    """Login via browser, capture the WebSocket authorization token."""
    from playwright.async_api import async_playwright

    for attempt in range(4):
        print(f"  browser login attempt {attempt+1}/4...", flush=True)
        async with async_playwright() as p:
            browser = await p.chromium.launch(
                headless=True,
                executable_path="/usr/bin/chromium",
                args=["--disable-blink-features=AutomationControlled", "--no-sandbox"],
            )
            ctx = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1440, "height": 900},
            )
            page = await ctx.new_page()
            captured: list[str] = []

            def on_ws(ws) -> None:
                def _f(payload) -> None:
                    if isinstance(payload, bytes):
                        try:
                            payload = payload.decode("utf-8", "replace")
                        except Exception:
                            return
                    if isinstance(payload, str) and 'authorization' in payload:
                        captured.append(payload)
                ws.on("framesent", _f)

            page.on("websocket", on_ws)
            try:
                await page.goto(LOGIN_PAGE_URL, wait_until="domcontentloaded", timeout=60_000)
                try:
                    await page.wait_for_selector('#tab-1 form[action$="/sign-in/"]', state="visible", timeout=20_000)
                except Exception:
                    print("    login form not found", flush=True)
                    await browser.close()
                    continue
                await page.fill('#tab-1 input[name="email"]', EMAIL)
                await page.fill('#tab-1 input[name="password"]', PASSWORD)
                try:
                    await page.click('#tab-1 form[action$="/sign-in/"] button[type="submit"]', timeout=8000)
                except Exception:
                    try:
                        await page.click('#tab-1 button:has-text("Sign in")', timeout=8000)
                    except Exception:
                        await page.evaluate(
                            "() => { const f=[...document.forms].find(x=>/sign-in/.test(x.action)); "
                            "const b=[...f.querySelectorAll('button')].find(x=>/sign in/i.test(x.innerText||'')); b&&b.click(); }")
                for _ in range(60):
                    if "/sign-in" not in page.url:
                        break
                    await asyncio.sleep(1)
                if "/sign-in" in page.url:
                    print("    login redirect failed (still on sign-in)", flush=True)
                    await browser.close()
                    continue
                await page.goto(DEMO_TRADE_PAGE_URL, wait_until="domcontentloaded", timeout=60_000)
                try:
                    await page.wait_for_load_state("networkidle", timeout=25_000)
                except Exception:
                    pass
                await asyncio.sleep(6)
            except Exception as exc:
                print(f"    browser error: {type(exc).__name__}: {exc}", flush=True)
                await browser.close()
                continue
            await browser.close()

            for line in captured:
                try:
                    name, payload = json.loads(line[2:])
                    if name == "authorization":
                        token = payload.get("session")
                        if token:
                            print(f"  got token: {token[:8]}...{token[-4:]}", flush=True)
                            return token
                except Exception:
                    continue
            print("    no authorization frame captured", flush=True)
    return None


async def test_recipe(
    ssid: str, asset: str, amount: int, steps: list[str], label: str, *, wait_depth: bool = False
) -> str:
    """Run one recipe on its own fresh connection."""
    ack = None
    depth_event = asyncio.Event()
    pending_header = None
    auth_sent = False
    auth_ready = asyncio.Event()
    server_error = None

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
            nonlocal pending_header, auth_sent, ack, server_error
            while True:
                try:
                    msg = await ws.recv()
                except websockets.ConnectionClosed:
                    return
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
                        depth_event.set()
                    if pending_header in ("instruments/list", "s_authorization"):
                        auth_ready.set()
                    continue
                if text.startswith("\x04"):
                    try:
                        payload = json.loads(text[1:])
                    except Exception:
                        pending_header = None
                        continue
                    if pending_header == "s_orders/open":
                        ack = json.dumps(payload)[:400]
                    pending_header = None
                    continue
                if text.startswith("42["):
                    try:
                        parts = json.loads(text[2:])
                        name, args = parts[0], parts[1:]
                    except Exception:
                        continue
                    if name == "instruments/list":
                        auth_ready.set()
                    if name == "s_authorization":
                        auth_ready.set()
                    if name in ("s_orders/open", "successopenOrder") and args:
                        if isinstance(args[0], dict):
                            ack = json.dumps(args[0])[:400]
                    if name == "error" and args:
                        server_error = json.dumps(args)[:200]
                    continue

        rt = asyncio.create_task(reader())

        try:
            await asyncio.wait_for(auth_ready.wait(), timeout=10)
        except asyncio.TimeoutError:
            rt.cancel()
            return "❌ AUTH TIMEOUT"

        # Run setup steps
        for step in steps:
            await ws.send(step)
            await asyncio.sleep(1.0)

        # Wait for depth if required
        if wait_depth:
            try:
                await asyncio.wait_for(depth_event.wait(), timeout=8)
                print(f"  [{label}] depth/change OK", flush=True)
            except asyncio.TimeoutError:
                print(f"  [{label}] depth/change TIMEOUT", flush=True)

        # Place order
        rid = int(time.time())
        order = build_order_message(
            asset=asset, amount=amount, time_field=60, action="put",
            is_demo=True, request_id=rid, option_type=100,
        )
        await ws.send(order)
        await asyncio.sleep(10)

        rt.cancel()

    if server_error:
        return f"⚠️  ERROR: {server_error}"
    if ack:
        return f"✅ ACK: {ack}"
    return "❌ DROPPED"


async def main() -> None:
    import websockets

    parser = argparse.ArgumentParser()
    parser.add_argument("--amount", type=int, default=10000)
    parser.add_argument("--asset", type=str, default="EURCAD_otc")
    args = parser.parse_args()

    asset = sanitize_asset(args.asset)
    amount = args.amount

    print("Getting fresh browser token...")
    token = await get_fresh_token()
    if not token:
        print("\n❌ Could not get fresh token. Browser login failed.")
        print("Check QSB_EMAIL / QSB_PASSWORD in .env and try again.")
        return

    print(f"\nAsset: {asset}, Amount: {amount}")
    print(f"Token: {token[:8]}...{token[-4:]}\n")

    # Recipe A: bot current (expected DROPPED)
    print("--- Recipe A: bot current (tick + instruments/follow + order) ---")
    r_a = await test_recipe(token, asset, amount, [
        build_tick_message(), build_follow_message(asset)
    ], "A")
    print(f"  Result: {r_a}\n")

    await asyncio.sleep(2)

    # Recipe B: minimal fix (tick + instruments/update + depth/follow + order)
    print("--- Recipe B: minimal fix (tick + instruments/update + depth/follow + order) ---")
    r_b = await test_recipe(token, asset, amount, [
        build_tick_message(),
        f'42["instruments/update",{{"asset":"{asset}","period":60}}]',
        f'42["depth/follow","{asset}"]',
    ], "B", wait_depth=True)
    print(f"  Result: {r_b}\n")

    await asyncio.sleep(2)

    # Recipe C: depth-only (is instruments/update needed?)
    print("--- Recipe C: depth-only (tick + depth/follow + order) ---")
    r_c = await test_recipe(token, asset, amount, [
        build_tick_message(),
        f'42["depth/follow","{asset}"]',
    ], "C", wait_depth=True)
    print(f"  Result: {r_c}\n")

    print("=" * 60)
    print(f"A (bot current):         {r_a}")
    print(f"B (inst_update + depth): {r_b}")
    print(f"C (depth only):          {r_c}")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(main())
