"""Drive the real Quotex demo-trade page headlessly and record EVERY WebSocket
frame (both directions) to debug/browser_frames.log, then place a Down trade
and dump the orders-related frames.

This is the ground truth for the current orders/open payload.
"""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv

load_dotenv()

EMAIL = os.environ.get("QSB_EMAIL", "")
PASSWORD = os.environ.get("QSB_PASSWORD", "")

from broker.protocol import DEMO_TRADE_PAGE_URL, LOGIN_PAGE_URL

LOG_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "browser_frames.log")

counter = {"frames": 0}


def _log_both(line: str) -> None:
    print(line, flush=True)
    with open(LOG_PATH, "a", encoding="utf-8") as fh:
        fh.write(line + "\n")


async def main() -> None:
    from playwright.async_api import async_playwright

    with open(LOG_PATH, "w", encoding="utf-8") as fh:
        fh.write("")

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
            url = ws.url
            _log_both(f"\n=== WEBSOCKET: {url}")

            def _frame(direction: str, payload) -> None:
                if isinstance(payload, bytes):
                    payload = payload.decode("utf-8", "replace")
                if not payload:
                    return
                counter["frames"] += 1
                if len(payload) > 1200:
                    payload = payload[:1200] + f"...<{len(payload)}>"
                _log_both(f"[{direction}] {payload}")

            ws.on("framesent", lambda f: _frame("SENT", f))
            ws.on("framereceived", lambda f: _frame("RECV", f))

        page.on("websocket", on_ws)
        page.on("console", lambda m: _log_both(f"[console:{m.type}] {m.text[:300]}") if m.type in ("error", "warning") else None)
        page.on("pageerror", lambda e: _log_both(f"[pageerror] {str(e)[:300]}"))

        _log_both(f"navigating to {LOGIN_PAGE_URL}")
        for attempt in range(3):
            await page.goto(LOGIN_PAGE_URL, wait_until="domcontentloaded", timeout=60_000)
            try:
                await page.wait_for_selector('#tab-1 form[action$="/sign-in/"]', state="visible", timeout=25_000)
                break
            except Exception as exc:
                _log_both(f"!! login form not found (attempt {attempt+1}): {type(exc).__name__}")
                if attempt == 2:
                    return

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
        _log_both(f"after login url: {page.url}")

        await page.goto(DEMO_TRADE_PAGE_URL, wait_until="domcontentloaded", timeout=60_000)
        try:
            await page.wait_for_load_state("networkidle", timeout=30_000)
        except Exception:
            pass
        _log_both(f"demo trade page loaded: {page.url}")
        await asyncio.sleep(10)

        before = counter["frames"]
        _log_both(f"frames so far: {before}")

        # Click Down (put) — force-dispatch to bypass overlay actionability
        _log_both("\n--- clicking Down ---")
        clicked = False
        try:
            btn = page.locator('button:has-text("Down")').first
            await btn.wait_for(state="visible", timeout=15000)
            await btn.click(force=True, timeout=8000)
            _log_both("clicked Down (force)")
            clicked = True
        except Exception as exc:
            _log_both(f"!! Down click failed: {type(exc).__name__}: {exc}")
        if not clicked:
            try:
                r = await page.evaluate("""() => {
                    const btns = [...document.querySelectorAll('button')];
                    const d = btns.find(b => /^down$/i.test((b.innerText||'').trim()));
                    if (!d) return 'no down button';
                    d.click();
                    return 'js-clicked ' + d.className.toString().slice(0,60);
                }""")
                _log_both(f"JS click: {r}")
            except Exception as exc:
                _log_both(f"!! JS click failed: {exc}")

        await asyncio.sleep(15)
        try:
            await page.screenshot(path="debug/after_trade.png", timeout=10000)
        except Exception as exc:
            _log_both(f"!! screenshot failed: {exc}")
        _log_both(f"frames after click+15s: {counter['frames']} (delta {counter['frames']-before})")

        # Inspect the trade area state after clicking
        try:
            state = await page.evaluate("""() => {
                const out = {betArea: [], positionCards: []};
                for (const el of document.querySelectorAll('button')) {
                    const t = (el.innerText||'').trim().replace(/\\n/g,' ').slice(0,40);
                    if (/up|down/i.test(t) && t.length < 20) out.betArea.push({cls:String(el.className).slice(0,50), text:t});
                }
                for (const el of document.querySelectorAll('*')) {
                    if (/open|closed|positions|ticket/i.test(String(el.className))) {
                        const t=(el.innerText||'').trim().replace(/\\n/g,' ').slice(0,60);
                        if (t) out.positionCards.push({cls:String(el.className).slice(0,50), text:t.slice(0,60)});
                    }
                }
                return out;
            }""")
            for k, v in state.items():
                _log_both(f"  {k}: {v[:12]}")
        except Exception as exc:
            _log_both(f"!! state inspect failed: {exc}")

        await browser.close()

    _log_both("\n=== orders-related frames from log ===")
    with open(LOG_PATH, encoding="utf-8") as fh:
        for line in fh:
            if "orders" in line or "error" in line.lower() or "SENT" in line:
                print(line.rstrip()[:500], flush=True)


if __name__ == "__main__":
    asyncio.run(main())
