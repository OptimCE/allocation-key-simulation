from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from shared.models.crm_models import AllocationKeyModel, IterationModel


class CRMRepository:
    def __init__(self, session: AsyncSession):
        self.session = session

    async def get_allocation_key(
        self, id_key: int, id_community: int
    ) -> AllocationKeyModel | None:
        """Load a CRM allocation key with its iteration/consumer subtree.

        Scoped to ``id_community`` explicitly (rather than via
        ``with_community_scope``) so the method is safe in both the request
        context (API) and the worker, which has no request-scoped community
        ContextVar. Filtering on ``id_community`` enforces tenant isolation —
        a key belonging to another community returns ``None``.

        Iterations/consumers are eager-loaded; callers must order iterations by
        ``number`` before running the simulation (the relationship is unordered).
        """
        stmt = (
            select(AllocationKeyModel)
            .options(
                selectinload(AllocationKeyModel.iterations).selectinload(
                    IterationModel.consumers
                )
            )
            .where(AllocationKeyModel.id == id_key)
            .where(AllocationKeyModel.id_community == id_community)
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
