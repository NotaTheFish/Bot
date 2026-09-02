"""
Титулы: каталог, выдача/изъятие, выбор активного.
Титулы от админа помечаются is_admin_grant (уникальные, редкие).
"""
import db


async def create_title(name: str, admin_grant: bool, by: int) -> int:
    """Создать титул в каталоге (или вернуть существующий с тем же именем)."""
    existing = await db.pool().fetchval(
        "SELECT id FROM rb_titles WHERE lower(name)=lower($1)", name)
    if existing:
        return existing
    return await db.pool().fetchval(
        "INSERT INTO rb_titles (name, is_admin_grant, created_by) VALUES ($1,$2,$3) RETURNING id",
        name, admin_grant, by)


async def grant_title(tg_id: int, title_id: int, by: int | None) -> bool:
    """Выдать титул игроку. by=None если от достижения."""
    await db.pool().execute(
        "INSERT INTO rb_user_titles (tg_id, title_id, granted_by) VALUES ($1,$2,$3) "
        "ON CONFLICT (tg_id, title_id) DO NOTHING", tg_id, title_id, by)
    return True


async def grant_title_by_name(tg_id: int, name: str, by: int | None,
                              admin_grant: bool = True) -> int:
    """Создать (если нужно) и выдать титул по имени. Возвращает title_id."""
    tid = await create_title(name, admin_grant, by or 0)
    await grant_title(tg_id, tid, by)
    return tid


async def revoke_title(tg_id: int, title_id: int):
    """Забрать титул у игрока. Если он был активным — сбросить."""
    await db.pool().execute(
        "DELETE FROM rb_user_titles WHERE tg_id=$1 AND title_id=$2", tg_id, title_id)
    await db.pool().execute(
        "UPDATE rb_users SET active_title=NULL WHERE tg_id=$1 AND active_title=$2",
        tg_id, title_id)


async def user_titles(tg_id: int) -> list[dict]:
    """Титулы игрока (с флагом admin_grant и активностью)."""
    rows = await db.pool().fetch(
        "SELECT t.id, t.name, t.is_admin_grant, ut.granted_by, "
        "(u.active_title = t.id) AS is_active "
        "FROM rb_user_titles ut JOIN rb_titles t ON t.id=ut.title_id "
        "JOIN rb_users u ON u.tg_id=ut.tg_id "
        "WHERE ut.tg_id=$1 ORDER BY ut.granted_at DESC", tg_id)
    return [dict(r) for r in rows]


async def set_active_title(tg_id: int, title_id: int | None) -> bool:
    """Выбрать активный титул (или снять, если None). Проверяет владение."""
    if title_id is not None:
        owns = await db.pool().fetchval(
            "SELECT 1 FROM rb_user_titles WHERE tg_id=$1 AND title_id=$2", tg_id, title_id)
        if not owns:
            return False
    await db.pool().execute(
        "UPDATE rb_users SET active_title=$1 WHERE tg_id=$2", title_id, tg_id)
    return True


async def titles_count(tg_id: int) -> int:
    return await db.pool().fetchval(
        "SELECT count(*) FROM rb_user_titles WHERE tg_id=$1", tg_id) or 0
