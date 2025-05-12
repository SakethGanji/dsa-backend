import functools
import importlib.resources as pkg
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

@functools.lru_cache(maxsize=None)
def _load_sql(fn: str) -> str:
    return pkg.read_text("app.users.sql", fn)

LIST_USERS_SQL   = _load_sql("list_users.sql")
CREATE_USER_SQL  = _load_sql("create_user.sql")

async def list_users(session: AsyncSession) -> list[dict]:
    result = await session.execute(text(LIST_USERS_SQL))
    return [dict(r._mapping) for r in result.fetchall()]

async def create_user(session: AsyncSession, soeid: str, password_hash: str, role_id: int) -> dict | None:
    async with session.begin():
        result = await session.execute(
            text(CREATE_USER_SQL),
            {"soeid": soeid, "password_hash": password_hash, "role_id": role_id}
        )
    row = result.first()
    return dict(row._mapping) if row else None
