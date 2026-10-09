"""Tests for the userbot group monitor: invite-hash parsing, config gating,
and routing of group messages into the shared signal handler."""

from __future__ import annotations

import asyncio
from datetime import time

import pytest

from broker.base import Direction
from telegram_signals.group_monitor import (
    GroupMonitor,
    extract_invite_hash,
    should_run,
)
from telegram_signals.handlers import SignalHandler

SAMPLE = """💰5-minute expiration
USD/IDR; 08:55; PUT 🟥

🕐 TIME TO 09:00

1st GALE —> TIME TO 09:05
2nd GALE —> TIME TO 09:10"""


class _FakeEvent:
    def __init__(self, text: str, chat_id: int = -1001234567890, msg_id: int = 42):
        class _Msg:
            def __init__(self) -> None:
                self.text = text
                self.id = msg_id

        self.message = _Msg()
        self.chat_id = chat_id


def test_extract_invite_hash_variants():
    assert extract_invite_hash("https://t.me/+68esYWwntAwwOWFh") == "68esYWwntAwwOWFh"
    assert extract_invite_hash("https://t.me/joinchat/AAAAabc123") == "AAAAabc123"
    assert extract_invite_hash("t.me/+abc") == "abc"
    assert extract_invite_hash("tg://join?invite=xyz789") == "xyz789"
    assert extract_invite_hash("https://t.me/+") is None       # empty hash
    assert extract_invite_hash("some_public_group") is None    # not an invite link
    assert extract_invite_hash("@username") is None


def test_should_run_requires_all_four():
    assert should_run("https://t.me/+abc", 111, "h", "+15551234567")
    assert not should_run("", 111, "h", "+15551234567")   # no link
    assert not should_run("https://t.me/+abc", 0, "h", "+15551234567")  # no api_id
    assert not should_run("https://t.me/+abc", 111, "", "+15551234567")  # no hash
    assert not should_run("https://t.me/+abc", 111, "h", "")  # no phone


def test_should_run_accepts_multiple_links():
    assert should_run(
        ("https://t.me/+abc", "https://t.me/+def"), 111, "h", "+15551234567"
    )
    assert not should_run((), 111, "h", "+15551234567")  # empty link list


async def test_on_message_routes_text_into_shared_handler():
    """A group message is parsed and enqueued exactly like a bot message."""
    queue: asyncio.Queue = asyncio.Queue()
    handler = SignalHandler(queue)
    monitor = GroupMonitor(
        handler,
        session_path="/tmp/nonexistent.session",
        api_id=111,
        api_hash="hash",
        phone="+15551234567",
        links=("https://t.me/+68esYWwntAwwOWFh",),
    )

    await monitor._on_message(_FakeEvent(SAMPLE))

    signal = queue.get_nowait()
    assert signal.asset == "USD/IDR"
    assert signal.direction is Direction.PUT
    assert signal.entry_time == time(8, 55)
    assert signal.source == "group:-1001234567890/msg:42"
    assert queue.empty()


async def test_process_text_dedup_shared_across_bot_and_monitor():
    """The same scheduled signal arriving via the PTB bot and via the monitor
    (same handler) is queued only once."""
    queue: asyncio.Queue = asyncio.Queue()
    handler = SignalHandler(queue, dedup_window_s=600.0)

    await handler.process_text(SAMPLE, source="chat:6249066789/msg:1")
    await handler.process_text(SAMPLE, source="group:-1001234567890/msg:2")

    assert queue.qsize() == 1
    assert queue.get_nowait().source == "chat:6249066789/msg:1"


def test_monitor_target_id_none_before_start():
    monitor = GroupMonitor(
        SignalHandler(asyncio.Queue()),
        session_path="/tmp/nonexistent.session",
        api_id=111,
        api_hash="hash",
        phone="+15551234567",
        links=("https://t.me/+abc",),
    )
    assert monitor.target_id is None


class _FakeEntity:
    def __init__(self, eid: int, title: str):
        self.id = eid
        self.title = title


class _FakeInviteResult:
    """Mimics Telethon >=1.44 ChatInviteJoinResultOk: the chat entity is wrapped
    under ``.updates.chats`` rather than exposed as ``.chats``."""

    def __init__(self, chat: _FakeEntity) -> None:
        class _Updates:
            def __init__(self, c: _FakeEntity) -> None:
                self.chats = [c]

        self.updates = _Updates(chat)


class _FakeClient:
    """Minimal Telethon stand-in: resolves links to entities, answers the join
    request, and records the event handler + the chats it was registered with."""

    def __init__(
        self,
        entities: dict[str, _FakeEntity],
        join_chat: _FakeEntity | None = None,
    ) -> None:
        self._entities = entities
        self._join_chat = join_chat
        self.started = False
        self.handler = None
        self.chats = None
        self.joined_hash: str | None = None

    async def start(self, phone: str | None = None) -> None:
        self.started = True

    async def get_entity(self, link: str) -> _FakeEntity:
        try:
            return self._entities[link]
        except KeyError:
            raise ValueError(link)

    async def __call__(self, request) -> _FakeInviteResult:
        self.joined_hash = request.hash
        return _FakeInviteResult(self._join_chat)

    def add_event_handler(self, handler, event) -> None:
        self.handler = handler
        self.chats = event.chats


async def test_start_resolves_every_link_and_listens_to_all():
    """start() resolves each configured link and registers one handler whose
    chats cover every target, so all groups feed the shared handler."""
    queue: asyncio.Queue = asyncio.Queue()
    monitor = GroupMonitor(
        SignalHandler(queue),
        session_path="/tmp/nonexistent.session",
        api_id=111,
        api_hash="hash",
        phone="+15551234567",
        links=("https://t.me/+aaa", "https://t.me/+bbb"),
    )
    client = _FakeClient({
        "https://t.me/+aaa": _FakeEntity(1111, "First"),
        "https://t.me/+bbb": _FakeEntity(2222, "Second"),
    })
    monitor._client = client

    await monitor.start()

    assert client.started
    assert monitor.target_id == 1111
    assert [t.id for t in monitor._targets] == [1111, 2222]
    assert sorted(c.id for c in client.chats) == [1111, 2222]


async def test_join_path_unwraps_chat_from_invite_result():
    """When the account isn't a member yet (get_entity raises ValueError), the
    monitor joins via ImportChatInviteRequest and unwraps the chat from the
    Telethon >=1.44 ChatInviteJoinResultOk wrapper."""
    queue: asyncio.Queue = asyncio.Queue()
    monitor = GroupMonitor(
        SignalHandler(queue),
        session_path="/tmp/nonexistent.session",
        api_id=111,
        api_hash="hash",
        phone="+15551234567",
        links=("https://t.me/+abc",),
    )
    chat = _FakeEntity(7777, "Joined")
    client = _FakeClient({}, join_chat=chat)  # get_entity fails for every link
    monitor._client = client

    await monitor.start()

    assert client.joined_hash == "abc"
    assert monitor.target_id == 7777
    assert [c.id for c in client.chats] == [7777]
