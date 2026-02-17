from __future__ import annotations

from aiogram import Dispatcher, Router

router = Router(name="listener")


def register_listener_handlers(dispatcher: Dispatcher) -> None:
    dispatcher.include_router(router)