# shared/middleware/gateway_scope.py
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response

from core.context_vars import (
    current_community_id,
    current_source_ip,
    current_user_id,
    current_user_role,
)


class GatewayScopeMiddleware(BaseHTTPMiddleware):
    """
    Sets ContextVars from the KrakenD-forwarded headers so that
    services, repositories, and audit logging can read them
    without passing UserContext explicitly.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        # Build user context from headers (won't raise for unauthenticated routes).
        # Starlette headers are case-insensitive and return None for missing keys.
        user_id = request.headers.get("x-user-id")
        community_id = request.headers.get("x-community-id")
        role = request.headers.get("x-user-orgs")  # or derive from orgs if needed
        source_ip = request.headers.get("x-source-ip")

        t1 = current_user_id.set(user_id)
        t2 = current_community_id.set(community_id)
        t3 = current_user_role.set(role)
        t4 = current_source_ip.set(source_ip)

        try:
            return await call_next(request)
        finally:
            current_user_id.reset(t1)
            current_community_id.reset(t2)
            current_user_role.reset(t3)
            current_source_ip.reset(t4)
