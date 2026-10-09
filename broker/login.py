"""Quotex email/password login (token capture).

The modern Quotex API authenticates the WebSocket with a *token* minted when
you sign in on the website — there is no clean JSON login endpoint any more.
This module recovers that token three ways:

1. **httpx fast path** (default, pyquotex-style): replicates the flow that
   pyquotex uses successfully — GET the home page for cookies, GET
   ``/sign-in/modal/`` for the CSRF ``_token``, POST the credentials, detect a
   ``keep_code`` field meaning a one-time PIN (2FA) is required, POST the PIN
   to ``/sign-in/modal``, then read ``window.settings.token`` (or
   ``/api/v1/cabinets/digest``) from the trade page. Uses a Firefox TLS
   fingerprint (cipher suite + ECDH curve) and an optional proxy so plain
   Python httpx isn't challenged by Cloudflare.
2. **cloudscraper fast path** (fallback): scrapes the sign-in page CSRF and
   POSTs the credentials. Needs ``cloudscraper`` + ``beautifulsoup4``.
3. **Playwright fallback**: drives a real browser when the above trip
   anti-bot protection. Needs ``playwright`` installed.

The OTP (2FA) PIN, when required, is bridged to Telegram via ``otp_bridge``
(:mod:`telegram_signals.otp_bridge`): the bridge sends the prompt and awaits
the user's reply. ``demo`` selects the demo vs live account — each account
type has its own token, which is how mode switching works.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import ssl
from typing import Any

import httpx

from broker.base import BrokerAuthError
from broker.protocol import LOGIN_PAGE_URL, TRADE_PAGE_URL, DEMO_TRADE_PAGE_URL

logger = logging.getLogger(__name__)

_TOKEN_RE = re.compile(r"window\.settings\s*=\s*(\{.*?\})", re.S)

_BASE_URL = "https://quotex.com"
_LANG = "en"
_UA = "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:109.0) Gecko/20100101 Firefox/119.0"

# Firefox TLS fingerprint (cipher order + ECDH curve) so Cloudflare doesn't
# challenge the plain-Python TLS handshake. Mirrors pyquotex's ssl_utils.
_CIPHER_SUITE_FIREFOX = (
    "TLS_AES_128_GCM_SHA256:"
    "TLS_CHACHA20_POLY1305_SHA256:"
    "TLS_AES_256_GCM_SHA384:"
    "ECDHE-ECDSA-AES128-GCM-SHA256:"
    "ECDHE-RSA-AES128-GCM-SHA256:"
    "ECDHE-ECDSA-CHACHA20-POLY1305:"
    "ECDHE-RSA-CHACHA20-POLY1305:"
    "ECDHE-ECDSA-AES256-GCM-SHA384:"
    "ECDHE-RSA-AES256-GCM-SHA384:"
    "ECDHE-ECDSA-AES256-SHA:"
    "ECDHE-ECDSA-AES128-SHA:"
    "ECDHE-RSA-AES128-SHA:"
    "ECDHE-RSA-AES256-SHA:"
    "DHE-RSA-AES128-SHA:"
    "DHE-RSA-AES256-SHA:"
    "AES128-SHA:"
    "AES256-SHA:"
    "DES-CBC3-SHA"
)

_BROWSER_HEADERS = {
    "User-Agent": _UA,
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,*/*;q=0.8"),
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-User": "?1",
}


def _firefox_ssl_context() -> ssl.SSLContext:
    """SSL context with Firefox's cipher list + ECDH curve (TLS fingerprint)."""
    import certifi

    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = True
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_verify_locations(certifi.where())
    ctx.set_ciphers(_CIPHER_SUITE_FIREFOX)
    try:
        ctx.set_ecdh_curve("prime256v1")
    except (ValueError, KeyError, ssl.SSLError):
        pass
    ctx.minimum_version = ssl.TLSVersion.TLSv1_2
    return ctx


def _soup(html: str) -> Any:
    from bs4 import BeautifulSoup

    return BeautifulSoup(html, "html.parser")


class _QuotexSession:
    """Minimal pyquotex-style HTTP session: Firefox TLS + headers + proxy."""

    def __init__(self, proxy: str = "") -> None:
        self.client = httpx.AsyncClient(
            verify=_firefox_ssl_context(),
            timeout=30.0,
            follow_redirects=True,
            proxy=proxy or None,
            headers=dict(_BROWSER_HEADERS),
        )

    async def aclose(self) -> None:
        await self.client.aclose()


def _extract_token(html: str) -> str | None:
    """Parse ``window.settings = {...}`` for ``token``, with a brace-matching
    fallback when the JSON contains nested braces (the ``.*?`` regex stops at
    the first ``}``)."""
    m = _TOKEN_RE.search(html)
    if m:
        try:
            data = json.loads(m.group(1))
        except (ValueError, TypeError):
            data = None
        if isinstance(data, dict) and data.get("token"):
            return data["token"]
    # Fallback: scan from "window.settings = {" to its matching brace.
    start = html.find("window.settings")
    if start >= 0:
        brace = html.find("{", start)
        if brace >= 0:
            depth = 0
            for i in range(brace, min(len(html), brace + 200_000)):
                ch = html[i]
                if ch == "{":
                    depth += 1
                elif ch == "}":
                    depth -= 1
                    if depth == 0:
                        try:
                            data = json.loads(html[brace : i + 1])
                        except (ValueError, TypeError):
                            data = None
                        if isinstance(data, dict) and data.get("token"):
                            return data["token"]
                        break
    return None


def _csrf_from_modal(html: str) -> str | None:
    """CSRF ``_token`` from the ``/sign-in/modal/`` page (pyquotex flow)."""
    m = _soup(html).find("input", {"name": "_token"})
    return m["value"] if m and m.get("value") else None


def _otp_prompt(html: str) -> str:
    """Human prompt from the OTP form (the page's ``<p>`` text if present)."""
    try:
        auth = _soup(html).find("main", {"class": "auth__body"})
        p = auth.find("p") if auth else None
        if p and p.text.strip():
            return f"{p.text.strip()}: "
    except Exception:
        pass
    return "Enter the OTP code sent to your email: "


async def http_login_via_http(
    email: str,
    password: str,
    demo: bool,
    *,
    otp_bridge: Any = None,
    proxy: str = "",
) -> str:
    """pyquotex-style login over httpx (Firefox TLS + optional proxy).

    Returns the Quotex session token. ``otp_bridge`` is the Telegram OTP
    bridge used when the account requires a one-time PIN (2FA).
    """
    target = DEMO_TRADE_PAGE_URL if demo else TRADE_PAGE_URL
    session = _QuotexSession(proxy)
    try:
        # 1. Home page — establishes the session cookies.
        home = await session.client.get(f"{_BASE_URL}/{_LANG}")
        home.raise_for_status()

        # 2. Sign-in modal — CSRF token for the login form.
        modal = await session.client.get(f"{_BASE_URL}/{_LANG}/sign-in/modal/")
        csrf = _csrf_from_modal(modal.text)
        if not csrf:
            raise BrokerAuthError("could not find CSRF token on Quotex sign-in page")

        data = {"_token": csrf, "email": email, "password": password, "remember": 1}
        resp = await session.client.post(f"{_BASE_URL}/{_LANG}/sign-in/", data=data)

        # 3. OTP (2FA): the response carries a keep_code input when a PIN is
        #    required. Bridge the code over Telegram, then resubmit.
        if _soup(resp.text).find("input", {"name": "keep_code"}) is not None:
            if otp_bridge is None:
                raise BrokerAuthError(
                    "Quotex requires an OTP PIN but no OTP bridge is wired "
                    "(QSB_EMAIL/QSB_PASSWORD login cannot complete)"
                )
            code = await otp_bridge.request_pin(_otp_prompt(resp.text))
            data["code"] = code
            data["keep_code"] = 1
            resp = await session.client.post(
                f"{_BASE_URL}/{_LANG}/sign-in/modal", data=data
            )

        # 4. Land on the trade page and read the token.
        if "trade" not in str(resp.url):
            resp = await session.client.get(target)
        token = _extract_token(resp.text)
        if not token:
            digest = await session.client.get(
                f"{_BASE_URL}/api/v1/cabinets/digest",
                headers={"Referer": f"{_BASE_URL}/{_LANG}/trade"},
            )
            if digest.is_success:
                token = (digest.json().get("data") or {}).get("token")
        if token:
            return token
        for name in ("session", "ssid", "qx_session"):
            val = session.client.cookies.get(name)
            if val:
                return val
        raise BrokerAuthError("logged in but no session token could be captured")
    finally:
        await session.aclose()


def http_login(email: str, password: str, demo: bool) -> str:
    """Synchronous cloudscraper login. Returns the Quotex session token."""
    import cloudscraper

    target = DEMO_TRADE_PAGE_URL if demo else TRADE_PAGE_URL
    scraper = cloudscraper.create_scraper()
    headers = {"Referer": LOGIN_PAGE_URL}

    page = scraper.get(LOGIN_PAGE_URL, timeout=30, headers=headers)
    csrf = _extract_csrf(page.text)
    if not csrf:
        raise BrokerAuthError("could not find CSRF token on Quotex sign-in page")

    resp = scraper.post(
        LOGIN_PAGE_URL,
        data={"_token": csrf, "email": email, "password": password, "remember": "1"},
        headers={"Referer": LOGIN_PAGE_URL, "Content-Type": "application/x-www-form-urlencoded"},
        timeout=30,
        allow_redirects=False,
    )
    if resp.status_code in (301, 302, 303, 307, 308):
        loc = resp.headers.get("Location")
        if loc:
            next_url = loc if loc.startswith("http") else "https://quotex.com" + loc
            try:
                scraper.get(next_url, timeout=30)
            except Exception:  # redirect chain is best-effort
                pass
    if resp.status_code not in (200, 301, 302, 303, 307, 308):
        raise BrokerAuthError(f"Quotex login rejected (HTTP {resp.status_code})")

    trade_html = scraper.get(target, timeout=30, headers=headers).text
    token = _extract_token(trade_html)
    if token:
        return token
    for name in ("session", "ssid", "qx_session"):
        val = scraper.cookies.get(name)
        if val:
            return val
    raise BrokerAuthError("logged in but no session token could be captured")


def _extract_csrf(html: str) -> str | None:
    """CSRF token from the sign-in page form (legacy cloudscraper path)."""
    try:
        from bs4 import BeautifulSoup

        form = BeautifulSoup(html, "html.parser").select_one('#tab-1 form[action$="/sign-in/"]')
        if form:
            hidden = form.select_one('input[name="_token"]')
            if hidden and hidden.get("value"):
                return hidden["value"]
    except ImportError:
        pass
    m = _TOKEN_RE.search(html)
    return m.group(1) if m else None


async def http_login_async(
    email: str,
    password: str,
    demo: bool,
    *,
    otp_bridge: Any = None,
    proxy: str = "",
) -> str:
    """Async login. Prefers the pyquotex-style httpx flow (handles OTP via the
    bridge + proxy), falls back to cloudscraper/Playwright when it fails for a
    reason other than a required OTP."""
    try:
        return await http_login_via_http(
            email, password, demo, otp_bridge=otp_bridge, proxy=proxy
        )
    except BrokerAuthError as fast_err:
        # A required-but-unanswered OTP can't be helped by the fallbacks.
        if "OTP" in str(fast_err):
            raise
        logger.warning("httpx login failed (%s); trying cloudscraper", fast_err)
    try:
        import cloudscraper  # noqa: F401  (import just to check availability)
    except ImportError:
        return await playwright_login(email, password, demo)
    try:
        return await asyncio.to_thread(http_login, email, password, demo)
    except BrokerAuthError as fast_err:
        logger.warning("cloudscraper login failed (%s); trying Playwright", fast_err)
        try:
            return await playwright_login(email, password, demo)
        except BrokerAuthError:
            raise fast_err


async def playwright_login(email: str, password: str, demo: bool) -> str:
    """Async Playwright fallback for when cloudscraper is blocked."""
    try:
        from playwright.async_api import async_playwright
    except ImportError as exc:
        raise BrokerAuthError(
            "automated login failed; install playwright (pip install playwright && "
            "playwright install chromium) or paste a token from your browser instead"
        ) from exc

    target = DEMO_TRADE_PAGE_URL if demo else TRADE_PAGE_URL
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--disable-blink-features=AutomationControlled"],
        )
        try:
            page = await (await browser.new_context()).new_page()
            await page.goto(LOGIN_PAGE_URL, wait_until="domcontentloaded", timeout=45_000)
            await page.wait_for_selector('#tab-1 form[action$="/sign-in/"]', state="visible", timeout=30_000)
            await page.fill('#tab-1 input[name="email"]', email)
            await page.fill('#tab-1 input[name="password"]', password)
            await page.click('#tab-1 form[action$="/sign-in/"] button[type="submit"]')
            try:
                await page.wait_for_url("**/(trade|demo-trade|cabinet)**", timeout=60_000)
            except Exception:
                pass
            await page.goto(target, wait_until="domcontentloaded", timeout=45_000)
            await page.wait_for_load_state("networkidle", timeout=20_000)
            token = await page.evaluate("() => window?.settings?.token ?? null")
            if token:
                return token
        finally:
            await browser.close()
    raise BrokerAuthError("Playwright login succeeded but no token was found")
