"""
Устарело: зеркала клиентов обслуживаются в одном процессе с `bot_controller.main_core`
(мульти-tenant SaaS). Отдельный деплой контроллера клиенту больше не требуется.

Для отладки можно временно задать те же переменные, что и раньше, и поднять только
экземпляр из этого файла — в продакшене используйте основной контроллер.
"""

from __future__ import annotations

import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger("tenant_main_deprecated")


def main() -> None:
    logger.error(
        "tenant_main: этот entry point устарел. Запустите основной контроллер "
        "(python -m bot_controller.main_core) — зеркальные боты поднимаются там."
    )
    sys.exit(2)


if __name__ == "__main__":
    main()
