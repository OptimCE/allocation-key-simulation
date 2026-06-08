from typing import cast

from sqlalchemy import func, select
from sqlalchemy.orm import selectinload

from core.database.with_community import with_community_scope
from shared.models.local_models import (
    SimulationIterationResultModel,
    SimulationKeyResultModel,
    SimulationModel,
)


class SimulationRepository:
    def __init__(self, session):
        self.session = session

    async def create_simulation(self, model: SimulationModel) -> SimulationModel:
        """Insert a SimulationModel and flush so the caller can read its id.

        The caller (service) owns the commit boundary so row creation and the
        NATS publish share a single atomic unit.
        """
        self.session.add(model)
        await self.session.flush()
        return model

    async def get_list_simulations(
        self, page: int, page_size: int, query_param: dict
    ) -> tuple[list[SimulationModel], int]:
        stmt = select(SimulationModel)
        stmt = with_community_scope(stmt, SimulationModel)

        if query_param:
            name = query_param.get("name")
            if name:
                stmt = stmt.where(SimulationModel.name.ilike(f"%{name}%"))
            status = query_param.get("status")
            if status:
                stmt = stmt.where(SimulationModel.status == int(status))
            sort_map = {
                "id": SimulationModel.id,
                "name": SimulationModel.name,
                "status": SimulationModel.status,
            }
            sort_clauses = []
            for key, column in sort_map.items():
                direction = query_param.get(f"sort_{key}")
                if not direction:
                    continue
                if direction.lower() == "desc":
                    sort_clauses.append(column.desc())
                elif direction.lower() == "asc":
                    sort_clauses.append(column.asc())
            if sort_clauses:
                stmt = stmt.order_by(*sort_clauses)
            else:
                stmt = stmt.order_by(SimulationModel.id.desc())
        else:
            stmt = stmt.order_by(SimulationModel.id.desc())

        total_result = await self.session.execute(select(func.count()).select_from(stmt.subquery()))
        total = total_result.scalar_one()

        rows_result = await self.session.execute(
            stmt.offset((page - 1) * page_size).limit(page_size)
        )
        rows = list(rows_result.scalars().all())
        return rows, total

    async def get_simulation(self, id: int) -> SimulationModel | None:
        """Fetch a simulation with its full scalar result tree eager-loaded."""
        stmt = (
            select(SimulationModel)
            .options(
                selectinload(SimulationModel.key_result)
                .selectinload(SimulationKeyResultModel.iterations)
                .selectinload(SimulationIterationResultModel.consumers)
            )
            .where(SimulationModel.id == id)
        )
        stmt = with_community_scope(stmt, SimulationModel)
        result = await self.session.execute(stmt)
        return cast(SimulationModel | None, result.scalar_one_or_none())

    async def get_simulation_row(self, id: int) -> SimulationModel | None:
        """Fetch just the simulation row (no result tree) for delete / timeseries."""
        stmt = select(SimulationModel).where(SimulationModel.id == id)
        stmt = with_community_scope(stmt, SimulationModel)
        result = await self.session.execute(stmt)
        return cast(SimulationModel | None, result.scalar_one_or_none())

    async def delete_simulation(self, simulation: SimulationModel) -> None:
        await self.session.delete(simulation)
        await self.session.flush()
