"""The simulation-key part of a data export (api/data_export.py).

Through the kit's own contract check (`run_contract_check`): the answer matches the contract,
every file listed was written, and each JSON file is checked here against its item schema
(core/export/schemas/files/simulation-key/).
"""

from __future__ import annotations

import json
import pathlib
import uuid
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker
from referencing import Registry, Resource
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from api.data_export import TABLES, export_handler
from core.export import ExportHandler, run_contract_check
from tests.factories.simulation_factory import create_simulation_with_result
from tests.factories.subscription_factory import create_community, create_subscription

pytestmark = pytest.mark.asyncio

SERVICE = "simulation-key"
SCHEMAS = pathlib.Path(__file__).resolve().parent.parent / f"core/export/schemas/files/{SERVICE}"
BUCKET = "crm-files"


def handler_for(session: AsyncSession, objects: dict[str, int]) -> ExportHandler:
    @asynccontextmanager
    async def shared() -> AsyncIterator[AsyncSession]:
        yield session

    async def size_of(key: str) -> int | None:
        return objects.get(key)

    return export_handler(local=shared, crm=shared, size_of=size_of, bucket=BUCKET)


def _validators() -> dict[str, Draft202012Validator]:
    schemas = [json.loads(p.read_text(encoding="utf-8")) for p in SCHEMAS.glob("*.schema.json")]
    registry: Registry = Registry().with_resources(
        (schema["$id"], Resource.from_contents(schema)) for schema in schemas
    )
    return {
        schema["title"].split("/")[1].removesuffix(".json item"): Draft202012Validator(
            schema, registry=registry, format_checker=FormatChecker()
        )
        for schema in schemas
        if schema.get("title", "").endswith(" item")
    }


async def export(
    session: AsyncSession,
    *,
    scope: dict[str, str],
    requester: str,
    objects: dict[str, int] | None = None,
) -> tuple[dict[str, Any], dict[str, list[dict[str, Any]]]]:
    """Runs one part; returns part.done and each JSON file's items, checked against its schema."""
    result = await run_contract_check(
        handler_for(session, objects or {}), service=SERVICE, scope=scope, requester_id=requester
    )
    assert result.outcome == "done", result.message
    validators = _validators()
    files: dict[str, list[dict[str, Any]]] = {}
    for file in result.message["files"]:
        document = json.loads(result.store.text(file["key"]))
        entity = document["entity"]
        errors = [
            f"{entity}[{i}]: {error.message}"
            for i, item in enumerate(document["items"])
            for error in validators[entity].iter_errors(item)
        ]
        assert not errors, errors
        files[entity] = document["items"]
    return result.message, files


def counters(done: dict[str, Any]) -> dict[str, int]:
    prefix = f"export.{SERVICE}."
    return {item["labelKey"].removeprefix(prefix): item["count"] for item in done["summary"]}


async def account(session: AsyncSession, community: Any, sub: str, role: str) -> None:
    user = int(
        (
            await session.execute(
                text("INSERT INTO app_user (auth_user_id, email) VALUES (:s, :e) RETURNING id"),
                {"s": sub, "e": f"{sub}@example.test"},
            )
        ).scalar_one()
    )
    await session.execute(
        text("INSERT INTO community_user (id_community, id_user, role) VALUES (:c, :u, :r)"),
        {"c": community.id, "u": user, "r": role},
    )
    await session.flush()


async def world(session: AsyncSession) -> dict[str, Any]:
    """A community with one finished simulation (its result tree), an admin and a manager."""
    community = await create_community(session, auth_community_id=f"comm-{uuid.uuid4().hex[:6]}")
    await create_subscription(session, id_community=community.id, feature="simulation")
    await account(session, community, "user-admin", "ADMIN")
    await account(session, community, "user-manager", "MANAGER")
    simulation = await create_simulation_with_result(
        session,
        id_community=community.id,
        result_storage_key=f"simulations/{community.id}/1/result.json",
    )
    return {"community": community, "simulation": simulation}


def community_scope(w: dict[str, Any]) -> dict[str, str]:
    return {"type": "community", "id": w["community"].auth_community_id}


class TestCommunityExport:
    async def test_every_table_codes_by_name_files_by_reference(self, db_session):
        w = await world(db_session)
        simulation = w["simulation"]
        done, files = await export(
            db_session,
            scope=community_scope(w),
            requester="user-admin",
            objects={simulation.file_storage_key: 1234},
        )

        assert set(files) == {table.entity for table in TABLES}
        assert [
            len(files[e])
            for e in ("simulations", "key-results", "iteration-results", "consumer-results")
        ] == [1, 1, 1, 1]
        (item,) = files["simulations"]
        assert item["status"] == "SUCCESS" and item["source"] in ("FILE", "CRM")
        assert "id_community" not in json.dumps(files)
        assert item["input_file"] == {
            "path": f"{SERVICE}/files/{simulation.id}-input-data.csv",
            "file_missing": False,
        }
        # result.json is not in this store: listed missing, not fatal.
        assert item["timeseries_file"] == {"path": None, "file_missing": True}
        assert done["references"] == [
            {
                "bucket": BUCKET,
                "key": simulation.file_storage_key,
                "targetPath": f"{SERVICE}/files/{simulation.id}-input-data.csv",
                "size": 1234,
            }
        ]
        assert counters(done) == {"simulations": 1, "files": 1, "files_missing": 1}

    async def test_a_nan_result_is_written_as_null(self, db_session):
        """JSON cannot carry NaN: the file would be unreadable by any strict parser."""
        w = await world(db_session)
        await db_session.execute(
            text(
                "UPDATE simulation_key_result SET sharing_rate_total = 'NaN' "
                "WHERE id_simulation = :s"
            ),
            {"s": w["simulation"].id},
        )
        _done, files = await export(db_session, scope=community_scope(w), requester="user-admin")
        (key_result,) = files["key-results"]
        assert key_result["sharing_rate_total"] is None
        assert key_result["surplus_total"] == 20.0

    async def test_the_module_off_hides_nothing(self, db_session):
        w = await world(db_session)
        await db_session.execute(
            text("UPDATE community_subscription SET is_active = false WHERE id_community = :c"),
            {"c": w["community"].id},
        )
        _done, files = await export(db_session, scope=community_scope(w), requester="user-admin")
        assert len(files["simulations"]) == 1

    async def test_only_an_admin_exports_the_community(self, db_session):
        w = await world(db_session)
        result = await run_contract_check(
            handler_for(db_session, {}),
            service=SERVICE,
            scope=community_scope(w),
            requester_id="user-manager",
        )
        assert result.outcome == "failed"
        assert result.message["errorCode"] == "forbidden"

    async def test_an_unknown_community_is_scope_not_found(self, db_session):
        await world(db_session)
        result = await run_contract_check(
            handler_for(db_session, {}),
            service=SERVICE,
            scope={"type": "community", "id": str(uuid.uuid4())},
            requester_id="user-admin",
        )
        assert result.outcome == "failed"
        assert result.message["errorCode"] == "scope_not_found"


class TestUserExport:
    async def test_a_user_export_holds_nothing_of_this_service(self, db_session):
        await world(db_session)
        done, files = await export(
            db_session, scope={"type": "user", "id": "user-admin"}, requester="user-admin"
        )
        assert files == {} and done["files"] == [] and done["references"] == []
