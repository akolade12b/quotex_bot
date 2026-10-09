"""Capture the browser's exact SENT frame sequence during a real trade, then
replay that sequence verbatim over a raw WebSocket connection.

If the replay ACKs, the message sequence is the difference (fixable in the bot).
If it drops, the transport (TLS/cookies/HTTP2) is the difference and the bot
needs the browser path.
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

EMAIL = os.environ.get("QSB_EMAIL", "")
PASSWORD = os.environ.get("QSB_PASSWORD", "")

from broker.protocol import DEMO_TRADE_PAGE_URL, LOGIN_PAGE_URL, SOCKETIO_ENDPOINT

SENT_LOG = os.path.join(os.path.dirname(os.path.abspath(__file__)), "browser_sent_seq.log")


async def capture_browser_sequence() -> tuple[str, list[str]] | None:
    from playwright.async_api import async_playwright

    for outer in range(4):
        sent: list[str] = []
        token = None
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

            def on_ws(ws) -> None:
                nonlocal token
                def _f(payload) -> None:
                    if isinstance(payload, bytes):
                        try:
                            payload = payload.decode("utf-8", "replace")
                        except Exception:
                            return
                    if not isinstance(payload, str):
                        return
                    if payload.startswith("42"):
                        sent.append(payload)
                        if 'authorization' in payload:
                            try:
                                token = json.loads(payload[2:])[1]["session"]
                            except Exception:
                                pass

                ws.on("framesent", _f)

            page.on("websocket", on_ws)

            ok = False
            for attempt in range(3):
                try:
                    await page.goto(LOGIN_PAGE_URL, wait_until="domcontentloaded", timeout=60_000)
                    await page.wait_for_selector('#tab-1 form[action$="/sign-in/"]', state="visible", timeout=20_000)
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
                        continue
                    await page.goto(DEMO_TRADE_PAGE_URL, wait_until="domcontentloaded", timeout=60_000)
                    try:
                        await page.wait_for_load_state("networkidle", timeout=25_000)
                    except Exception:
                        pass
                    await asyncio.sleep(12)
                    base_len = len(sent)
                    await page.evaluate(
                        "() => { const b=[...document.querySelectorAll('button')].find(x=>/^down$/i.test((x.innerText||'').trim())); if(b) b.click(); }")
                    await asyncio.sleep(10)
                    print(f"attempt {outer+1}: sent {len(sent)} frames (pre-trade {base_len})", flush=True)
                    ok = True
                    break
                except Exception as exc:
                    print(f"attempt {outer+1} flow failed: {type(exc).__name__}", flush=True)
                    continue
            if not ok:
                await browser.close()
                continue
            await browser.close()
            if not token:
                print("no token captured; sent frames:", flush=True)
                for s in sent:
                    print("   ", s[:120], flush=True)
                return None
            auth_idx = next((i for i, s in enumerate(sent) if 'authorization' in s), 0)
            tail = sent[auth_idx:]
            with open(SENT_LOG, "w", encoding="utf-8") as fh:
                for s in tail:
                    fh.write(s + "\n")
            print("captured tail:", len(tail), "frames", flush=True)
            return token, tail
    return None


async def replay_raw(ssid: str, sequence: list[str]) -> None:
    import websockets
    from broker.protocol import build_authorization_message

    acks: list[str] = []
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
                    continue
                if text.startswith("\x04"):
                    try:
                        payload = json.loads(text[1:])
                    except Exception:
                        continue
                    if pending_header == "s_orders/open":
                        acks.append(payload.get("id"))
                    pending_header = None
                    continue

        rt = asyncio.create_task(reader())
        await asyncio.sleep(3)
        for frame in sequence:
            if 'authorization' in frame:
                # already authed via build_authorization_message above; skip
                continue
            await ws.send(frame)
            await asyncio.sleep(0.8)
        # the order is in the sequence; give it time
        await asyncio.sleep(15)
        print("REPLAY RESULT:", acks[0] if acks else "DROPPED", flush=True)
        rt.cancel()


async def main() -> None:
    captured = await capture_browser_sequence()
    if not captured:
        print("capture failed", flush=True)
        return
    token, sequence = captured
    print("\n--- replaying on raw WS ---", flush=True)
    await replay_raw(token, sequence)


if __name__ == "__main__":
    asyncio.run(main())
