"""Tests for the Telegram OTP bridge: Future-based PIN wait, the message
handler consuming an OTP reply instead of a signal, and the pyquotex-style
login's OTP branch."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from broker import login as login_mod
from broker.base import BrokerAuthError
from telegram_signals.bot import build_bot
from telegram_signals.handlers import SignalHandler
from telegram_signals.otp_bridge import OtpBridge


async def test_bridge_resolves_on_submit():
    bridge = OtpBridge(timeout_s=5.0)
    task = asyncio.create_task(bridge.request_pin("Enter PIN: "))
    await asyncio.sleep(0.01)
    assert bridge.awaiting
    assert bridge.submit_pin("123456") is True
    assert await task == "123456"
    assert not bridge.awaiting


async def test_bridge_submit_without_pending_is_ignored():
    bridge = OtpBridge()
    assert bridge.submit_pin("123456") is False


async def test_bridge_times_out():
    bridge = OtpBridge(timeout_s=0.05)
    with pytest.raises(BrokerAuthError):
        await bridge.request_pin("Enter PIN: ")


async def test_bridge_rejects_nonnumeric_reply():
    bridge = OtpBridge(timeout_s=5.0)
    task = asyncio.create_task(bridge.request_pin("Enter PIN: "))
    await asyncio.sleep(0.01)
    bridge.submit_pin("not-a-number")
    with pytest.raises(BrokerAuthError):
        await task


def _update(text: str, chat_id: int, msg_id: int = 7) -> SimpleNamespace:
    msg = SimpleNamespace(text=text, chat_id=chat_id, message_id=msg_id)
    return SimpleNamespace(effective_message=msg)


async def test_handler_consumes_otp_reply_not_signal():
    queue: asyncio.Queue = asyncio.Queue()
    bridge = OtpBridge(timeout_s=5.0)
    handler = SignalHandler(
        queue, otp_bridge=bridge,
        allowed_chat_ids=(-100,), allowlist_enabled=True,
    )
    task = asyncio.create_task(bridge.request_pin("Enter PIN: "))
    await asyncio.sleep(0.01)
    await handler.handle_signal_message(_update("123456", chat_id=-100), None)
    assert await task == "123456"
    assert queue.empty()  # consumed as OTP, never queued as a signal


async def test_handler_still_parses_signals_when_not_awaiting_otp():
    queue: asyncio.Queue = asyncio.Queue()
    bridge = OtpBridge()
    handler = SignalHandler(
        queue, otp_bridge=bridge,
        allowed_chat_ids=(-100,), allowlist_enabled=True,
    )
    text = "💰5-minute expiration\nUSD/IDR; 08:55; PUT 🟥"
    await handler.handle_signal_message(_update(text, chat_id=-100), None)
    assert queue.qsize() == 1


async def test_build_bot_attaches_otp_bridge_to_prebuilt_handler():
    """Regression: main.py passes a pre-built shared handler to build_bot. The
    bridge must be attached to it or an OTP reply is misparsed as a signal
    (observed live: the PIN reply hit 'asset not found')."""
    queue: asyncio.Queue = asyncio.Queue()
    bridge = OtpBridge()
    handler = SignalHandler(queue, allowed_chat_ids=(-100,), allowlist_enabled=True)
    assert handler.otp_bridge is None  # created without a bridge, like main.py

    build_bot(
        "token:12345",
        queue,
        allowed_chat_ids=(-100,),
        handler=handler,
        otp_bridge=bridge,
    )

    assert handler.otp_bridge is bridge
    task = asyncio.create_task(bridge.request_pin("Enter PIN: "))
    await asyncio.sleep(0.01)
    await handler.handle_signal_message(_update("161648", chat_id=-100), None)
    assert await task == "161648"


# ---------------------------------------------------------------- login OTP

class _FakeResp:
    def __init__(self, text: str = "", url: str = "", json_data=None,
                 is_success: bool = True):
        self.text = text
        self.url = url
        self._json = json_data or {}
        self.is_success = is_success

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self._json


class _FakeClient:
    """Scripted httpx client: pops the next response per call."""

    def __init__(self, responses: list[_FakeResp]):
        self._responses = list(responses)
        self.calls: list[tuple] = []
        self.cookies = SimpleNamespace(get=lambda name: None)

    async def get(self, url: str, headers=None):
        self.calls.append(("GET", url))
        return self._responses.pop(0)

    async def post(self, url: str, data=None):
        self.calls.append(("POST", url, dict(data or {})))
        return self._responses.pop(0)


class _FakeBridge:
    def __init__(self, code: str = "123456"):
        self.code = code
        self.prompts: list[str] = []

    async def request_pin(self, prompt: str) -> str:
        self.prompts.append(prompt)
        return self.code


def _patch_session(monkeypatch, client: _FakeClient) -> None:
    class _FakeSession:
        def __init__(self, proxy: str = ""):
            self.client = client

        async def aclose(self) -> None:
            pass

    monkeypatch.setattr(login_mod, "_QuotexSession", _FakeSession)


def test_extract_token_nested_braces():
    html = 'window.settings = {"a": {"b": 1}, "token": "tok-1"};'
    assert login_mod._extract_token(html) == "tok-1"


def test_csrf_from_modal():
    html = '<form><input name="_token" value="csrf-42"></form>'
    assert login_mod._csrf_from_modal(html) == "csrf-42"


async def test_http_login_otp_flow(monkeypatch):
    """Login asks for a PIN; the bridge supplies it; token is captured."""
    client = _FakeClient([
        _FakeResp(text="<html>home</html>", url="https://quotex.com/en"),
        _FakeResp(text='<input name="_token" value="csrf1">', url="https://quotex.com/en/sign-in/modal/"),
        # POST /sign-in/ responds with the OTP form (keep_code present).
        _FakeResp(text='<input name="keep_code"><main class="auth__body"><p>Check your email</p></main>',
                  url="https://quotex.com/en/sign-in/"),
        # POST /sign-in/modal with the code redirects into the trade page.
        _FakeResp(text='window.settings = {"token": "ssid-final"}',
                  url="https://quotex.com/en/trade"),
    ])
    _patch_session(monkeypatch, client)
    bridge = _FakeBridge(code="987654")

    token = await login_mod.http_login_via_http(
        "user@example.com", "pw", demo=True, otp_bridge=bridge
    )

    assert token == "ssid-final"
    assert bridge.prompts and "Check your email" in bridge.prompts[0]
    otp_post = next(c for c in client.calls if c[0] == "POST" and "modal" in c[1])
    assert otp_post[2]["code"] == "987654"
    assert otp_post[2]["keep_code"] == 1


async def test_http_login_no_otp(monkeypatch):
    """No keep_code in the response -> no PIN round-trip, token captured."""
    client = _FakeClient([
        _FakeResp(text="<html>home</html>", url="https://quotex.com/en"),
        _FakeResp(text='<input name="_token" value="csrf2">', url="https://quotex.com/en/sign-in/modal/"),
        _FakeResp(text='window.settings = {"token": "ssid-plain"}',
                  url="https://quotex.com/en/demo-trade"),
    ])
    _patch_session(monkeypatch, client)

    token = await login_mod.http_login_via_http("u@e.com", "pw", demo=True)

    assert token == "ssid-plain"
    assert not any("modal" in c[1] for c in client.calls if c[0] == "POST")


async def test_http_login_otp_requires_bridge(monkeypatch):
    client = _FakeClient([
        _FakeResp(text="<html>home</html>", url="https://quotex.com/en"),
        _FakeResp(text='<input name="_token" value="csrf3">', url="https://quotex.com/en/sign-in/modal/"),
        _FakeResp(text='<input name="keep_code">', url="https://quotex.com/en/sign-in/"),
    ])
    _patch_session(monkeypatch, client)

    with pytest.raises(BrokerAuthError, match="OTP"):
        await login_mod.http_login_via_http("u@e.com", "pw", demo=True)
