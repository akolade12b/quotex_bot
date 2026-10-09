"""Tests for the /mode and /balance Telegram command handlers (no network)."""

from __future__ import annotations

import pytest

from telegram_signals.commands import _balance, _mode


class FakeMessage:
    def __init__(self, text: str, chat_id: int = 111) -> None:
        self.text = text
        self.chat_id = chat_id
        self.replies: list[str] = []

    async def reply_text(self, text: str, **_kwargs) -> None:
        self.replies.append(text)


class FakeUpdate:
    def __init__(self, message: FakeMessage) -> None:
        self.effective_message = message


async def _run_mode(text, *, switch_mode=None, status_fn=None, chat_id=111,
                    allowed=(111,), enabled=True) -> FakeMessage:
    msg = FakeMessage(text, chat_id)
    await _mode(FakeUpdate(msg), None, switch_mode=switch_mode, status_fn=status_fn,
                allowed=allowed, enabled=enabled)
    return msg


async def test_mode_live_calls_switch_mode():
    calls = []

    async def switch(demo: bool):
        calls.append(demo)
        return ("quotex-live" if demo else "quotex-demo", 5000.0)

    async def status():
        return ("quotex-demo", 10000.0)

    msg = await _run_mode("/mode live", switch_mode=switch, status_fn=status)
    assert calls == [True]
    assert "quotex-live" in msg.replies[0]
    assert "5000.00" in msg.replies[0]


async def test_mode_demo_calls_switch_mode_false():
    calls = []

    async def switch(demo: bool):
        calls.append(demo)
        return ("quotex-demo", 10000.0)

    msg = await _run_mode("/mode demo", switch_mode=switch, status_fn=switch)
    assert calls == [False]


async def test_mode_without_arg_reports_status():
    async def status():
        return ("quotex-demo", 10000.0)

    msg = await _run_mode("/mode", status_fn=status)
    assert msg.replies == ["Mode: **quotex-demo** · balance **10000.00**"]


async def test_mode_without_switch_mode_reports_status_not_error():
    async def status():
        return ("simulated", 10000.0)

    msg = await _run_mode("/mode live", switch_mode=None, status_fn=status)
    assert msg.replies == ["Mode: **simulated** · balance **10000.00**"]


async def test_mode_switch_error_surfaced_to_user():
    async def switch(demo: bool):
        raise RuntimeError("refusing to switch while 2 trade(s) are open")

    msg = await _run_mode("/mode live", switch_mode=switch, status_fn=switch)
    assert "Could not switch" in msg.replies[0]
    assert "2 trade(s)" in msg.replies[0]


async def test_mode_unauthorized_chat_ignored():
    calls = []

    async def switch(demo: bool):
        calls.append(demo)
        return ("quotex-live", 1.0)

    msg = await _run_mode("/mode live", switch_mode=switch, chat_id=999,
                          allowed=(111,), enabled=True)
    assert calls == []
    assert msg.replies == []


async def test_balance_command_reports_status():
    async def status():
        return ("simulated", 123.5)

    msg = FakeMessage("/balance")
    await _balance(FakeUpdate(msg), None, status_fn=status,
                   allowed=(111,), enabled=True)
    assert msg.replies == ["Mode: **simulated** · balance **123.50**"]


async def test_balance_unauthorized_ignored():
    async def status():
        return ("simulated", 123.5)

    msg = FakeMessage("/balance", chat_id=999)
    await _balance(FakeUpdate(msg), None, status_fn=status,
                   allowed=(111,), enabled=True)
    assert msg.replies == []
