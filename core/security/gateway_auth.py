import logging
import re

from core.security.user_context import ROLE_HIERARCHY, OrgToken, Role

logger = logging.getLogger(__name__)


def parse_user_orgs(user_orgs: str) -> list[OrgToken]:
    """
    Parses the x-user-orgs header injected by KrakenD/Keycloak.

    Example input:
    [orgId:2c8a... orgPath:/aaaa roles:[ADMIN]],
    map[orgId:a221... orgPath:/bbbb roles:[MEMBER,MANAGER]]
    """
    # Match each [...] block
    matches = re.findall(r"\[([^\]]+)\]", user_orgs)
    tokens: list[OrgToken] = []

    for match in matches:
        parts = match.split()
        if len(parts) < 3:
            continue

        try:
            org_id = parts[0].split(":", 1)[1]
            org_path = parts[1].split(":", 1)[1]
            roles_raw = parts[2].split(":", 1)[1]
            # Strip surrounding brackets: [ADMIN,MANAGER] -> ADMIN,MANAGER
            roles_raw = roles_raw.strip("[]")
            role_list = [r.strip() for r in roles_raw.split(",") if r.strip()]
            highest = _resolve_highest_role(role_list)
            if highest is not None:
                tokens.append(OrgToken(org_id=org_id, org_path=org_path, role=highest))
        except (IndexError, ValueError) as e:
            logger.warning("Failed to parse org token", extra={"raw": match, "error": str(e)})
            continue

    return tokens


def _resolve_highest_role(roles: list[str]) -> Role | None:
    """Returns the highest-privilege role from a list of role strings."""
    best: Role | None = None
    best_rank = -1

    for r in roles:
        try:
            role = Role(r)
        except ValueError:
            continue
        rank = ROLE_HIERARCHY.get(role, 0)
        if rank > best_rank:
            best_rank = rank
            best = role

    return best
