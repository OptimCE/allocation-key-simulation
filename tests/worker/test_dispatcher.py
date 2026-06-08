"""Failure-path tests for the worker dispatcher.

The per-message handler is invoked directly with a ``FakeMsg`` so no live NATS
or Postgres is needed. Collaborators (``_snapshot_simulation``, ``storage``,
``_read_crm_key``, ``from_crm_allocation_key`` / ``consumer_names_of``,
``data_loading.load``, ``run_simulation``, ``persistence.save_*``) are patched
on the dispatcher module so the tests exercise only its branching.

Cleanup contract: ``storage.delete`` is called exactly once for every terminal
outcome (success, key-not-found, parse failure, compute failure). It is NOT
called on transient NAKs (the redelivered message still needs the file) nor
when the object was already missing.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import OperationalError

from core.queue.helper import Event
from worker import dispatcher

_KEY = "simulations/1/test-uuid/data.csv"


class FakeMsg:
    def __init__(self, data: bytes) -> None:
        self.data = data
        self.acked = False
        self.naked = False
        self.nak_delay: int | None = None

    async def ack(self) -> None:
        self.acked = True

    async def nak(self, delay: int | None = None) -> None:
        self.naked = True
        self.nak_delay = delay


def _event_bytes(simulation_id) -> bytes:
    return Event(type="simulation.requested", data={"simulation_id": simulation_id}).encode()


def _snapshot(*, status: int = 0, file_storage_key: str = _KEY) -> dispatcher._SimulationSnapshot:
    return dispatcher._SimulationSnapshot(
        id=1,
        file_storage_key=file_storage_key,
        file_name="data.csv",
        injection_name="production",
        id_key=10,
        id_community=1,
        status=int(status),
    )


@pytest.fixture
def patched_save(monkeypatch):
    save_success = AsyncMock()
    save_failure = AsyncMock()
    monkeypatch.setattr(dispatcher.persistence, "save_success", save_success)
    monkeypatch.setattr(dispatcher.persistence, "save_failure", save_failure)
    return save_success, save_failure


@pytest.fixture
def patched_storage(monkeypatch):
    download = AsyncMock(return_value=b"file-bytes")
    delete = AsyncMock()
    monkeypatch.setattr(dispatcher.storage, "download", download)
    monkeypatch.setattr(dispatcher.storage, "delete", delete)
    return download, delete


@pytest.fixture
def stub_pipeline(monkeypatch, patched_storage):
    """Default-success patches so each test overrides only the step it exercises."""
    monkeypatch.setattr(dispatcher, "_snapshot_simulation", AsyncMock(return_value=_snapshot()))
    monkeypatch.setattr(dispatcher, "_read_crm_key", AsyncMock(return_value=MagicMock()))
    monkeypatch.setattr(dispatcher, "from_crm_allocation_key", MagicMock(return_value=MagicMock()))
    monkeypatch.setattr(dispatcher, "consumer_names_of", MagicMock(return_value=["C0"]))
    monkeypatch.setattr(
        dispatcher.data_loading,
        "load",
        MagicMock(return_value=MagicMock(C=None, VA=None, consumer_names=["C0"])),
    )
    monkeypatch.setattr(dispatcher, "run_simulation", MagicMock(return_value=MagicMock()))


# ---- Malformed events ------------------------------------------------------


async def test_acks_and_drops_invalid_json(patched_save, patched_storage):
    save_success, save_failure = patched_save
    _download, delete = patched_storage
    await dispatcher._make_handler()(FakeMsg(b"not json"))
    # No exception, message dropped via ack — assert via a fresh msg
    msg = FakeMsg(b"not json")
    await dispatcher._make_handler()(msg)
    assert msg.acked and not msg.naked
    save_failure.assert_not_awaited()
    delete.assert_not_awaited()


async def test_acks_when_simulation_id_missing(patched_save, patched_storage):
    _ss, save_failure = patched_save
    _download, delete = patched_storage
    msg = FakeMsg(Event(type="simulation.requested", data={"oops": 1}).encode())
    await dispatcher._make_handler()(msg)
    assert msg.acked and not msg.naked
    save_failure.assert_not_awaited()
    delete.assert_not_awaited()


async def test_acks_when_simulation_id_not_int(patched_save, patched_storage):
    msg = FakeMsg(Event(type="simulation.requested", data={"simulation_id": "x"}).encode())
    await dispatcher._make_handler()(msg)
    assert msg.acked and not msg.naked


# ---- Happy path ------------------------------------------------------------


async def test_happy_path_acks_and_deletes(monkeypatch, patched_save, stub_pipeline):
    save_success, save_failure = patched_save
    delete = dispatcher.storage.delete
    msg = FakeMsg(_event_bytes(1))
    await dispatcher._make_handler()(msg)
    assert msg.acked and not msg.naked
    save_success.assert_awaited_once()
    save_failure.assert_not_awaited()
    delete.assert_awaited_once_with(_KEY)


async def test_already_non_pending_acks_without_processing(
    monkeypatch, patched_save, patched_storage
):
    save_success, save_failure = patched_save
    _download, delete = patched_storage
    monkeypatch.setattr(
        dispatcher, "_snapshot_simulation", AsyncMock(return_value=_snapshot(status=1))
    )
    msg = FakeMsg(_event_bytes(1))
    await dispatcher._make_handler()(msg)
    assert msg.acked
    save_success.assert_not_awaited()
    delete.assert_awaited_once_with(_KEY)  # idempotent cleanup


# ---- Deterministic failures (ack + FAILED + delete) ------------------------


async def test_key_not_found_marks_failed(monkeypatch, patched_save, stub_pipeline):
    save_success, save_failure = patched_save
    monkeypatch.setattr(dispatcher, "_read_crm_key", AsyncMock(return_value=None))
    msg = FakeMsg(_event_bytes(1))
    await dispatcher._make_handler()(msg)
    assert msg.acked and not msg.naked
    save_failure.assert_awaited_once()
    assert save_failure.await_args.args[1].startswith("key_not_found:")
    dispatcher.storage.delete.assert_awaited_once_with(_KEY)


async def test_parse_failure_marks_failed(monkeypatch, patched_save, stub_pipeline):
    save_success, save_failure = patched_save
    monkeypatch.setattr(
        dispatcher.data_loading,
        "load",
        MagicMock(side_effect=dispatcher.data_loading.ConsumerColumnsError("missing C1")),
    )
    msg = FakeMsg(_event_bytes(1))
    await dispatcher._make_handler()(msg)
    assert msg.acked and not msg.naked
    save_failure.assert_awaited_once()
    assert save_failure.await_args.args[1].startswith("parse_failed:")
    dispatcher.storage.delete.assert_awaited_once_with(_KEY)


async def test_compute_error_marks_failed(monkeypatch, patched_save, stub_pipeline):
    save_success, save_failure = patched_save
    monkeypatch.setattr(
        dispatcher,
        "run_simulation",
        MagicMock(side_effect=dispatcher.SimulationInputError("bad key")),
    )
    msg = FakeMsg(_event_bytes(1))
    await dispatcher._make_handler()(msg)
    assert msg.acked and not msg.naked
    save_failure.assert_awaited_once()
    assert save_failure.await_args.args[1].startswith("simulation_failed:")
    dispatcher.storage.delete.assert_awaited_once_with(_KEY)


async def test_storage_object_missing_marks_failed_no_delete(
    monkeypatch, patched_save, patched_storage
):
    save_success, save_failure = patched_save
    download, delete = patched_storage
    download.side_effect = dispatcher.storage.ObjectNotFound("gone")
    monkeypatch.setattr(dispatcher, "_snapshot_simulation", AsyncMock(return_value=_snapshot()))
    msg = FakeMsg(_event_bytes(1))
    await dispatcher._make_handler()(msg)
    assert msg.acked
    save_failure.assert_awaited_once_with(1, "storage_object_missing")
    delete.assert_not_awaited()


# ---- Transient failures (nak + NO delete) ----------------------------------


async def test_storage_transient_naks_no_delete(monkeypatch, patched_save, patched_storage):
    save_success, save_failure = patched_save
    download, delete = patched_storage
    download.side_effect = dispatcher.storage.TransientStorageError("503")
    monkeypatch.setattr(dispatcher, "_snapshot_simulation", AsyncMock(return_value=_snapshot()))
    msg = FakeMsg(_event_bytes(1))
    await dispatcher._make_handler()(msg)
    assert msg.naked and not msg.acked
    assert msg.nak_delay == dispatcher._NAK_RETRY_DELAY_SECONDS
    save_failure.assert_not_awaited()
    delete.assert_not_awaited()


async def test_crm_read_error_naks_no_delete(monkeypatch, patched_save, stub_pipeline):
    save_success, save_failure = patched_save
    monkeypatch.setattr(
        dispatcher,
        "_read_crm_key",
        AsyncMock(side_effect=OperationalError("SELECT 1", {}, Exception("crm down"))),
    )
    msg = FakeMsg(_event_bytes(1))
    await dispatcher._make_handler()(msg)
    assert msg.naked and not msg.acked
    save_failure.assert_not_awaited()
    dispatcher.storage.delete.assert_not_awaited()


async def test_persist_failure_naks_no_delete(monkeypatch, patched_save, stub_pipeline):
    save_success, save_failure = patched_save
    save_success.side_effect = OperationalError("UPDATE", {}, Exception("conn lost"))
    msg = FakeMsg(_event_bytes(1))
    await dispatcher._make_handler()(msg)
    assert msg.naked and not msg.acked
    assert msg.nak_delay == dispatcher._NAK_RETRY_DELAY_SECONDS
    dispatcher.storage.delete.assert_not_awaited()


# ---- Unhandled error in snapshot (catch-all → ack + FAILED, no delete) -----


async def test_snapshot_raises_acks_with_unhandled_error(
    monkeypatch, patched_save, patched_storage
):
    save_success, save_failure = patched_save
    _download, delete = patched_storage
    monkeypatch.setattr(
        dispatcher,
        "_snapshot_simulation",
        AsyncMock(side_effect=OperationalError("SELECT 1", {}, Exception("db down"))),
    )
    msg = FakeMsg(_event_bytes(55))
    await dispatcher._make_handler()(msg)
    assert msg.acked and not msg.naked
    save_failure.assert_awaited_once_with(55, "unhandled_worker_error")
    delete.assert_not_awaited()
