from __future__ import annotations

from aiogram import Dispatcher, Router

router = Router(name="admin")


def register_admin_handlers(dispatcher: Dispatcher) -> None:
    dispatcher.include_router(router)