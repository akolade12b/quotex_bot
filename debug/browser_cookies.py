"""Log into Quotex in a real browser, dump the quotex.com cookies, then use
those cookies on a raw WebSocket connection to place an order — testing whether
the platform gates trading on the HTTP cookies of the WS upgrade handshake."""

from __future__ import annotations

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

from broker.protocol import DEMO_TRADE_PAGE_URL, LOGIN_PAGE_URL, SOCKETIO_ENDPOINT

from playwright.async_api import async_playwright


async def get_cookies() -> list[dict]:
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
        await page.goto(LOGIN_PAGE_URL, wait_until="domcontentloaded", timeout=60_000)
        for attempt in range(3):
            try:
                await page.wait_for_selector('#tab-1 form[action$="/sign-in/"]', state="visible", timeout=25_000)
                break
            except Exception:
                pass
        try:
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
        except Exception as exc:
            print("login failed:", exc, flush=True)
            await browser.close()
            return []
        await page.goto(DEMO_TRADE_PAGE_URL, wait_until="domcontentloaded", timeout=60_000)
        try:
            await page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass
        await asyncio.sleep(5)
        cookies = await ctx.cookies("https://quotex.com")
        await browser.close()
        return cookies


async def ws_trade_with_cookies(cookies: list[dict]) -> None:
    import websockets
    from broker.protocol import build_authorization_message

    cookie_str = "; ".join(f"{c['name']}={c['value']}" for c in cookies)
    print("cookies:", cookie_str[:200], flush=True)

    ssid = next((c["value"] for c in cookies if c["name"] in ("session", "ssid", "qx_session", "SID")), "")
    print("cookie session:", ssid[:8] if ssid else "(none)", flush=True)

    async with websockets.connect(
        SOCKETIO_ENDPOINT,
        origin="https://quotex.com",
        user_agent_header=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
        ),
        additional_headers={
            "Referer": "https://quotex.com/",
            "Accept-Language": "en-US,en;q=0.9",
            "Cookie": cookie_str,
        },
        ping_interval=None,
    ) as ws:
        auth_sent = False
        pending_header = None
        acked: list[dict] = []

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
                    await ws.send(build_authorization_message(ssid or "", True))
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
                        acked.append(payload)
                        print(f"!! ACK {payload.get('id')} req={payload.get('requestId')} "
                              f"price={payload.get('openPrice')} bal={payload.get('accountBalance')}", flush=True)
                    pending_header = None

        rt = asyncio.create_task(reader())
        await asyncio.sleep(2)
        rid = int(time.time())
        frame = '42["orders/open",{"asset":"EURCAD_otc","amount":10000,"time":60,' \
                f'"action":"put","isDemo":1,"tournamentId":0,"requestId":{rid},"optionType":100}}]'
        print("sending:", frame, flush=True)
        await ws.send(frame)
        await asyncio.sleep(12)
        print("ACKED" if acked else "DROPPED", flush=True)
        rt.cancel()


async def main() -> None:
    cookies = await get_cookies()
    if not cookies:
        print("no cookies", flush=True)
        return
    await ws_trade_with_cookies(cookies)


if __name__ == "__main__":
    asyncio.run(main())
