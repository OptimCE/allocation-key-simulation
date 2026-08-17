"""Unit tests for the shared ``core/realtime`` package.

This file is NOT part of the byte-identity contract (only the four
``core/realtime/*.py`` modules are — see ``scripts/check-realtime-parity.sh``),
but the behaviour it pins is shared by all five producers.
"""

import asyncio
import json

import pytest

from core.realtime import (
    MAX_ENVELOPE_BYTES,
    CommunityAudience,
    Tier,
    UserAudience,
    UsersAudience,
    build_envelope,
    bus,
    community_channel,
    user_channel,
)


class FakeRedis:
    """Records publishes. ``fail``/``hang`` drive the two failure paths."""

    def __init__(self, *, fail: bool = False, hang: bool = False) -> None:
        self.published: list[tuple[str, str]] = []
        self.fail = fail
        self.hang = hang

    async def publish(self, channel: str, body: bytes | str) -> int:
        if self.fail:
            raise ConnectionError("redis down")
        if self.hang:
            await asyncio.sleep(30)
        self.published.append((channel, body if isinstance(body, str) else body.decode()))
        return 1


@pytest.fixture
def fake_bus(monkeypatch):
    """Point emit() at a fake client with the feature switched on."""

    def _make(**kwargs):
        client = FakeRedis(**kwargs)
        monkeypatch.setattr(bus.settings, "REALTIME_ENABLED", True, raising=False)
        monkeypatch.setattr(bus.settings, "REALTIME_REDIS_URL", "redis://fake/1", raising=False)
        monkeypatch.setattr(bus, "_get_client", lambda: client)
        return client

    return _make


# ---- channels ------------------------------------------------------------


def test_channel_strings_match_the_typescript_contract():
    # These literals are the contract with crm-backend's realtime.channels.ts.
    # Changing one side without the other stops delivery silently: there is no
    # handshake, the events just go to a channel nobody listens on.
    assert user_channel(4821) == "notify:v1:u:4821"
    assert community_channel(12, Tier.MEMBER) == "notify:v1:c:12:MEMBER"
    assert community_channel(12, Tier.MANAGER) == "notify:v1:c:12:MANAGER"


def test_audiences_expand_to_the_right_channels():
    assert UserAudience(user_id=7).channels() == ("notify:v1:u:7",)
    # Duplicates collapse: publishing twice to one user would double-toast.
    assert UsersAudience(user_ids=[7, 8, 7]).channels() == ("notify:v1:u:7", "notify:v1:u:8")
    assert CommunityAudience(community_id=3, tier=Tier.MANAGER).channels() == (
        "notify:v1:c:3:MANAGER",
    )


def test_tier_has_no_default():
    # Required argument by design: a producer that publishes a per-user thing
    # onto a community tier is a cross-tenant leak, and omission must not be a
    # way to get there.
    with pytest.raises(TypeError):
        CommunityAudience(community_id=3)


# ---- envelope ------------------------------------------------------------


def test_build_envelope_stamps_the_wire_fields():
    env = build_envelope(
        topic="simulation.finished",
        resource=("simulation", 418),
        hint={"status": "success"},
        scope_community_id=12,
    )
    assert env is not None
    assert env["v"] == 1
    assert len(env["id"]) == 16
    assert env["ref"] == {"kind": "simulation", "id": "418"}
    assert env["scope"] == {"community_id": 12}
    assert env["at"].endswith("Z")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"topic": "not.a.topic", "resource": ("x", 1)},
        {"topic": "simulation.finished", "resource": ("", 1)},
        {"topic": "simulation.finished", "resource": ("x", 1), "hint": {"nested": {"a": 1}}},
        {"topic": "simulation.finished", "resource": ("x", 1), "hint": {"list": [1, 2]}},
    ],
)
def test_build_envelope_returns_none_instead_of_raising(kwargs):
    # Every caller runs AFTER a commit. An exception here would travel up through
    # a commit path, which is what the whole ordering design exists to prevent.
    assert build_envelope(**kwargs) is None


def test_build_envelope_rejects_oversize_in_bytes():
    env = build_envelope(
        topic="simulation.finished",
        resource=("simulation", 1),
        hint={"blob": "x" * MAX_ENVELOPE_BYTES},
    )
    assert env is None


# ---- emit ----------------------------------------------------------------


async def test_emit_is_a_no_op_when_unconfigured(monkeypatch):
    # The default everywhere realtime is not deployed — including every test
    # suite, which must never open a socket.
    created = []
    monkeypatch.setattr(bus.settings, "REALTIME_ENABLED", False, raising=False)
    monkeypatch.setattr(bus.settings, "REALTIME_REDIS_URL", "", raising=False)
    monkeypatch.setattr(bus, "_get_client", lambda: created.append(1))

    await bus.emit(
        topic="simulation.finished", audience=UserAudience(user_id=1), resource=("simulation", 1)
    )
    assert created == []


async def test_emit_publishes_one_message_per_channel(fake_bus):
    client = fake_bus()
    await bus.emit(
        topic="simulation.finished",
        audience=UsersAudience(user_ids=[1, 2]),
        resource=("simulation", 418),
        hint={"status": "success"},
    )
    assert [c for c, _ in client.published] == ["notify:v1:u:1", "notify:v1:u:2"]
    assert json.loads(client.published[0][1])["hint"] == {"status": "success"}


async def test_emit_swallows_a_broker_failure(fake_bus):
    # Fire-and-forget by contract: the caller has already committed, so a broker
    # problem must cost freshness and nothing else.
    fake_bus(fail=True)
    await bus.emit(
        topic="simulation.finished", audience=UserAudience(user_id=1), resource=("simulation", 1)
    )


async def test_emit_is_bounded_when_the_broker_hangs(fake_bus):
    # A *connected but hung* broker is the case a socket timeout does not cover.
    # Without the asyncio.timeout this would stall the worker indefinitely.
    fake_bus(hang=True)
    await asyncio.wait_for(
        bus.emit(
            topic="simulation.finished",
            audience=UserAudience(user_id=1),
            resource=("simulation", 1),
        ),
        timeout=5,
    )


async def test_emit_drops_a_malformed_envelope_without_publishing(fake_bus):
    client = fake_bus()
    await bus.emit(
        topic="simulation.finished",
        audience=UserAudience(user_id=1),
        resource=("simulation", 1),
        hint={"nested": {"a": 1}},
    )
    assert client.published == []
