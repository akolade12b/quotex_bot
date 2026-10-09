"""With a FRESH token and CORRECT binary-header parsing, test several order
recipes (each on its own connection) to find a reliable one."""

from __future__ import annotations

import asyncio
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

from broker.protocol import SOCKETIO_ENDPOINT

EMAIL = os.environ.get("QSB_EMAIL", "")
PASSWORD = os.environ.get("QSB_PASSWORD", "")

ASSET = "EURCAD_otc"


def recipe(name: str, steps: list[str]) -> None:
    pass


async def robust_browser_token() -> str | None:
    from playwright.async_api import async_playwright
    from broker.protocol import DEMO_TRADE_PAGE_URL, LOGIN_PAGE_URL

    for attempt in range(4):
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
                    await browser.close()
                    continue
                await page.fill('#tab-1 input[name="email"]', EMAIL)
                await page.fill('#tab-1 input[name="password"]', PASSWORD)
                try:
                    await page.click('#tab-1 form[action$="/sign-in/"] button[type="submit"]', timeout=8000)
                except Exception:
                    await page.click('#tab-1 button:has-text("Sign in")', timeout=8000)
                for _ in range(60):
                    if "/sign-in" not in page.url:
                        break
                    await asyncio.sleep(1)
                if "/sign-in" in page.url:
                    await browser.close()
                    continue
                await page.goto(DEMO_TRADE_PAGE_URL, wait_until="domcontentloaded", timeout=60_000)
                await asyncio.sleep(6)
            except Exception:
                await browser.close()
                continue
            await browser.close()
            for line in captured:
                try:
                    name, payload = json.loads(line[2:])
                    if name == "authorization":
                        return payload.get("session")
                except Exception:
                    continue
    return None


async def one_connection(ssid: str, label: str, setup: list[str], wait_depth: bool) -> str:
    import websockets
    from broker.protocol import build_authorization_message, build_tick_message

    depth_ready = asyncio.Event()
    ack: list[str] = []
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
                    await ws.send(build_authorization_message(ssid, True))
                    continue
                if text.startswith("451-["):
                    pending_header = text.split('451-["', 1)[1].split('"', 1)[0]
                    if pending_header == "depth/change":
                        depth_ready.set()
                    continue
                if text.startswith("\x04"):
                    try:
                        payload = json.loads(text[1:])
                    except Exception:
                        continue
                    if pending_header == "s_orders/open":
                        ack.append(payload.get("id"))
                    pending_header = None
                    continue

        rt = asyncio.create_task(reader())
        await asyncio.sleep(3)
        await ws.send(build_tick_message())
        for m in setup:
            await ws.send(m)
            await asyncio.sleep(1.0)
        if wait_depth:
            try:
                await asyncio.wait_for(depth_ready.wait(), timeout=8)
                print(f"  [{label}] depth ACTIVE", flush=True)
            except asyncio.TimeoutError:
                print(f"  [{label}] depth NOT active", flush=True)
        rid = int(time.time())
        frame = ('42["orders/open",{"asset":"EURCAD_otc","amount":10000,"time":60,'
                 f'"action":"put","isDemo":1,"tournamentId":0,"requestId":{rid},"optionType":100}}]')
        await ws.send(frame)
        await asyncio.sleep(8)
        rt.cancel()
    return ack[0] if ack else ""


async def main() -> None:
    token = await robust_browser_token()
    if not token:
        print("no fresh token", flush=True)
        return
    print("fresh token:", token, flush=True)

    from broker.protocol import build_follow_message

    recipes = [
        ("A: tick+follow+order (bot)", [build_follow_message(ASSET)], False),
        ("B: tick+follow+depth+order", [build_follow_message(ASSET), f'42["depth/follow","{ASSET}"]'], True),
        ("C: tick+depth+follow+order", [f'42["depth/follow","{ASSET}"]', build_follow_message(ASSET)], True),
        ("D: tick+fullsetup+depth+order",
         ['42["pending/list"]', '42["balance/list"]', '42["indicator/list"]', '42["drawing/load"]',
          f'42["instruments/update",{{"asset":"{ASSET}","period":60}}]',
          '42["chart_notification/get",{"asset":"EURCAD_otc","version":"1.0.0"}]',
          f'42["depth/follow","{ASSET}"]'], True),
    ]
    for label, setup, wait_depth in recipes:
        result = await one_connection(token, label, setup, wait_depth)
        print(f"[{label}] -> {'ACK ' + result if result else 'DROPPED'}", flush=True)


if __name__ == "__main__":
    asyncio.run(main())
