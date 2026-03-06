from pathlib import Path
import textwrap, json, zipfile, os, shutil

base = Path("/mnt/data/safe_automation_template")
if base.exists():
    shutil.rmtree(base)
base.mkdir()

files = {
    "main.py": r'''
from __future__ import annotations

import asyncio
import signal

from config import AppConfig
from logger import setup_logging, get_logger
from manager import AutomationManager


async def _run() -> None:
    config = AppConfig.from_env()
    setup_logging(config.log_level)
    logger = get_logger(__name__)

    manager = AutomationManager(config)
    await manager.start()

    stop_event = asyncio.Event()

    def _handle_stop(*_: object) -> None:
        logger.info("Shutdown signal received.")
        stop_event.set()

    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is not None:
            signal.signal(sig, _handle_stop)

    logger.info("Safe automation template started. Press Ctrl+C to stop.")
    await stop_event.wait()

    await manager.stop()
    logger.info("Stopped cleanly.")


if __name__ == "__main__":
    asyncio.run(_run())
''',

    "config.py": r'''
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    return int(raw)


@dataclass(slots=True)
class AppConfig:
    db_path: Path
    log_level: str
    worker_count: int
    task_poll_interval_sec: int
    screenshot_interval_sec: int
    screenshot_width: int
    screenshot_height: int
    ocr_region_x: int
    ocr_region_y: int
    ocr_region_w: int
    ocr_region_h: int
    use_real_screen: bool

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            db_path=Path(os.getenv("DB_PATH", "automation.db")),
            log_level=os.getenv("LOG_LEVEL", "INFO").upper(),
            worker_count=_env_int("WORKER_COUNT", 2),
            task_poll_interval_sec=_env_int("TASK_POLL_INTERVAL_SEC", 2),
            screenshot_interval_sec=_env_int("SCREENSHOT_INTERVAL_SEC", 5),
            screenshot_width=_env_int("SCREENSHOT_WIDTH", 1280),
            screenshot_height=_env_int("SCREENSHOT_HEIGHT", 720),
            ocr_region_x=_env_int("OCR_REGION_X", 20),
            ocr_region_y=_env_int("OCR_REGION_Y", 20),
            ocr_region_w=_env_int("OCR_REGION_W", 220),
            ocr_region_h=_env_int("OCR_REGION_H", 60),
            use_real_screen=_env_bool("USE_REAL_SCREEN", False),
        )
''',

    "logger.py": r'''
from __future__ import annotations

import logging
import sys


def setup_logging(level: str = "INFO") -> None:
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
        force=True,
    )


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)
''',

    "storage.py": r'''
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        finally:
            conn.close()

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS workers (
                    worker_id TEXT PRIMARY KEY,
                    status TEXT NOT NULL,
                    last_seen TEXT,
                    processed_count INTEGER NOT NULL DEFAULT 0
                );

                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'queued',
                    assigned_worker TEXT,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    result_json TEXT
                );

                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    worker_id TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    source TEXT NOT NULL,
                    note TEXT,
                    extracted_text TEXT
                );
                """
            )

    def seed_demo_tasks(self) -> None:
        from datetime import datetime
        import json

        demo_tasks = [
            {"task_type": "capture_screen", "payload_json": json.dumps({"note": "demo capture"})},
            {"task_type": "ocr_region", "payload_json": json.dumps({"note": "read numeric value"})},
            {"task_type": "health_check", "payload_json": json.dumps({"note": "verify worker loop"})},
        ]
        now = datetime.utcnow().isoformat(timespec="seconds")
        with self.connect() as conn:
            count = conn.execute("SELECT COUNT(*) AS n FROM tasks").fetchone()["n"]
            if count == 0:
                conn.executemany(
                    """
                    INSERT INTO tasks (task_type, payload_json, status, assigned_worker, created_at, updated_at, result_json)
                    VALUES (:task_type, :payload_json, 'queued', NULL, :now, :now, NULL)
                    """,
                    [{**task, "now": now} for task in demo_tasks],
                )

    def register_worker(self, worker_id: str, status: str) -> None:
        from datetime import datetime

        now = datetime.utcnow().isoformat(timespec="seconds")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO workers (worker_id, status, last_seen, processed_count)
                VALUES (?, ?, ?, 0)
                ON CONFLICT(worker_id) DO UPDATE SET
                    status=excluded.status,
                    last_seen=excluded.last_seen
                """,
                (worker_id, status, now),
            )

    def heartbeat(self, worker_id: str, status: str) -> None:
        from datetime import datetime

        now = datetime.utcnow().isoformat(timespec="seconds")
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE workers
                SET status = ?, last_seen = ?
                WHERE worker_id = ?
                """,
                (status, now, worker_id),
            )

    def fetch_next_task(self, worker_id: str) -> sqlite3.Row | None:
        from datetime import datetime

        now = datetime.utcnow().isoformat(timespec="seconds")
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT *
                FROM tasks
                WHERE status = 'queued'
                ORDER BY id
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None

            conn.execute(
                """
                UPDATE tasks
                SET status = 'running',
                    assigned_worker = ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (worker_id, now, row["id"]),
            )
            return conn.execute("SELECT * FROM tasks WHERE id = ?", (row["id"],)).fetchone()

    def complete_task(self, task_id: int, result_json: str) -> None:
        from datetime import datetime

        now = datetime.utcnow().isoformat(timespec="seconds")
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET status = 'done',
                    updated_at = ?,
                    result_json = ?
                WHERE id = ?
                """,
                (now, result_json, task_id),
            )

    def fail_task(self, task_id: int, result_json: str) -> None:
        from datetime import datetime

        now = datetime.utcnow().isoformat(timespec="seconds")
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE tasks
                SET status = 'failed',
                    updated_at = ?,
                    result_json = ?
                WHERE id = ?
                """,
                (now, result_json, task_id),
            )

    def increment_processed_count(self, worker_id: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                UPDATE workers
                SET processed_count = processed_count + 1
                WHERE worker_id = ?
                """,
                (worker_id,),
            )

    def add_snapshot(self, worker_id: str, source: str, note: str, extracted_text: str | None) -> None:
        from datetime import datetime

        now = datetime.utcnow().isoformat(timespec="seconds")
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO snapshots (worker_id, created_at, source, note, extracted_text)
                VALUES (?, ?, ?, ?, ?)
                """,
                (worker_id, now, source, note, extracted_text),
            )

    def get_status_summary(self) -> dict:
        with self.connect() as conn:
            workers = [dict(row) for row in conn.execute("SELECT * FROM workers ORDER BY worker_id")]
            tasks = [dict(row) for row in conn.execute("SELECT * FROM tasks ORDER BY id")]
        return {"workers": workers, "tasks": tasks}
''',

    "mock_screen.py": r'''
from __future__ import annotations

from dataclasses import dataclass
from PIL import Image, ImageDraw, ImageFont
import random


@dataclass(slots=True)
class MockScreen:
    width: int
    height: int

    def capture(self) -> Image.Image:
        img = Image.new("RGB", (self.width, self.height), color=(25, 30, 40))
        draw = ImageDraw.Draw(img)

        for _ in range(12):
            x1 = random.randint(0, self.width - 120)
            y1 = random.randint(0, self.height - 80)
            x2 = x1 + random.randint(40, 220)
            y2 = y1 + random.randint(30, 160)
            draw.rectangle((x1, y1, x2, y2), outline=(90, 120, 180), width=2)

        value = random.randint(100, 9999)
        label = f"VALUE: {value}"
        draw.rounded_rectangle((20, 20, 240, 80), radius=8, fill=(240, 240, 240))
        draw.text((35, 35), label, fill=(10, 10, 10), font=ImageFont.load_default())

        return img
''',

    "vision.py": r'''
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from PIL import Image
import numpy as np


@dataclass(slots=True)
class VisionModule:
    def crop_region(self, image: Image.Image, x: int, y: int, w: int, h: int) -> Image.Image:
        return image.crop((x, y, x + w, y + h))

    def average_brightness(self, image: Image.Image) -> float:
        gray = image.convert("L")
        arr = np.asarray(gray, dtype=np.float32)
        return float(arr.mean())

    def save_debug(self, image: Image.Image, path: str) -> str:
        image.save(path)
        return path

    def dominant_size(self, image: Image.Image) -> tuple[int, int]:
        return image.size
''',

    "ocr.py": r'''
from __future__ import annotations

import re
from typing import Optional

from PIL import Image

try:
    import pytesseract
except ImportError:  # pragma: no cover
    pytesseract = None


class OCRModule:
    def extract_text(self, image: Image.Image) -> str:
        if pytesseract is None:
            return ""
        text = pytesseract.image_to_string(image, config="--psm 7")
        return text.strip()

    def extract_first_integer(self, image: Image.Image) -> int | None:
        text = self.extract_text(image)
        match = re.search(r"\d+", text)
        return int(match.group(0)) if match else None
''',

    "worker.py": r'''
from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass

from PIL import ImageGrab

from config import AppConfig
from logger import get_logger
from mock_screen import MockScreen
from ocr import OCRModule
from storage import Storage
from vision import VisionModule


@dataclass(slots=True)
class Worker:
    worker_id: str
    config: AppConfig
    storage: Storage

    def __post_init__(self) -> None:
        self.logger = get_logger(f"worker.{self.worker_id}")
        self.vision = VisionModule()
        self.ocr = OCRModule()
        self.mock_screen = MockScreen(self.config.screenshot_width, self.config.screenshot_height)
        self._running = False

    def _capture_screen(self):
        if self.config.use_real_screen:
            return ImageGrab.grab()
        return self.mock_screen.capture()

    async def run(self) -> None:
        self._running = True
        self.storage.register_worker(self.worker_id, "idle")
        self.logger.info("Worker started.")

        try:
            while self._running:
                self.storage.heartbeat(self.worker_id, "idle")
                task = self.storage.fetch_next_task(self.worker_id)

                if task is None:
                    await asyncio.sleep(self.config.task_poll_interval_sec)
                    continue

                await self._process_task(task)
        except asyncio.CancelledError:
            self.logger.info("Worker cancelled.")
            raise
        finally:
            self.storage.heartbeat(self.worker_id, "stopped")

    async def _process_task(self, task) -> None:
        task_id = int(task["id"])
        task_type = task["task_type"]
        payload = json.loads(task["payload_json"])
        self.storage.heartbeat(self.worker_id, "running")

        try:
            if task_type == "capture_screen":
                result = self._handle_capture_screen(payload)
            elif task_type == "ocr_region":
                result = self._handle_ocr_region(payload)
            elif task_type == "health_check":
                result = {"ok": True, "worker_id": self.worker_id}
            else:
                raise ValueError(f"Unknown task type: {task_type}")

            self.storage.complete_task(task_id, json.dumps(result, ensure_ascii=False))
            self.storage.increment_processed_count(self.worker_id)
            self.logger.info("Task %s done: %s", task_id, task_type)
        except Exception as exc:
            self.logger.exception("Task %s failed", task_id)
            self.storage.fail_task(task_id, json.dumps({"error": str(exc)}, ensure_ascii=False))
        finally:
            self.storage.heartbeat(self.worker_id, "idle")
            await asyncio.sleep(0)

    def _handle_capture_screen(self, payload: dict) -> dict:
        image = self._capture_screen()
        brightness = self.vision.average_brightness(image)
        width, height = self.vision.dominant_size(image)
        note = payload.get("note", "")
        self.storage.add_snapshot(
            worker_id=self.worker_id,
            source="real_screen" if self.config.use_real_screen else "mock_screen",
            note=note,
            extracted_text=None,
        )
        return {
            "width": width,
            "height": height,
            "average_brightness": round(brightness, 2),
            "source": "real_screen" if self.config.use_real_screen else "mock_screen",
        }

    def _handle_ocr_region(self, payload: dict) -> dict:
        image = self._capture_screen()
        region = self.vision.crop_region(
            image,
            self.config.ocr_region_x,
            self.config.ocr_region_y,
            self.config.ocr_region_w,
            self.config.ocr_region_h,
        )
        extracted_text = self.ocr.extract_text(region)
        extracted_number = self.ocr.extract_first_integer(region)
        note = payload.get("note", "")
        self.storage.add_snapshot(
            worker_id=self.worker_id,
            source="ocr_region",
            note=note,
            extracted_text=extracted_text,
        )
        return {
            "text": extracted_text,
            "number": extracted_number,
            "region": {
                "x": self.config.ocr_region_x,
                "y": self.config.ocr_region_y,
                "w": self.config.ocr_region_w,
                "h": self.config.ocr_region_h,
            },
        }

    def stop(self) -> None:
        self._running = False
''',

    "manager.py": r'''
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from config import AppConfig
from logger import get_logger
from storage import Storage
from worker import Worker


@dataclass
class AutomationManager:
    config: AppConfig
    storage: Storage = field(init=False)
    workers: list[Worker] = field(default_factory=list)
    tasks: list[asyncio.Task] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.storage = Storage(self.config.db_path)
        self.logger = get_logger(__name__)

    async def start(self) -> None:
        self.storage.init_db()
        self.storage.seed_demo_tasks()

        for index in range(1, self.config.worker_count + 1):
            worker = Worker(worker_id=f"worker-{index}", config=self.config, storage=self.storage)
            self.workers.append(worker)
            self.tasks.append(asyncio.create_task(worker.run()))

        self.logger.info("Started %s workers.", len(self.workers))

    async def stop(self) -> None:
        for worker in self.workers:
            worker.stop()

        for task in self.tasks:
            task.cancel()

        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)

        summary = self.storage.get_status_summary()
        self.logger.info("Final status summary: %s", summary)
''',

    "requirements.txt": r'''
numpy>=1.26
Pillow>=10.0
pytesseract>=0.3.10
python-dotenv>=1.0
''',

    ".env.example": r'''
DB_PATH=automation.db
LOG_LEVEL=INFO
WORKER_COUNT=2
TASK_POLL_INTERVAL_SEC=2
SCREENSHOT_INTERVAL_SEC=5
SCREENSHOT_WIDTH=1280
SCREENSHOT_HEIGHT=720
OCR_REGION_X=20
OCR_REGION_Y=20
OCR_REGION_W=220
OCR_REGION_H=60
USE_REAL_SCREEN=false
''',

    "README.md": r'''
# Safe Automation Template

Это безопасная замена исходной идеи: не игровой бот, а шаблон desktop automation с мок-экраном, OCR, SQLite и менеджером воркеров.

## Что внутри

- `main.py` — точка входа
- `config.py` — конфиг через env
- `logger.py` — логирование
- `storage.py` — SQLite и создание таблиц
- `manager.py` — запуск и остановка воркеров
- `worker.py` — обработка задач
- `vision.py` — базовые операции со скриншотом
- `ocr.py` — OCR-модуль
- `mock_screen.py` — генератор тестового экрана
- `.env.example` — пример переменных окружения

## Что делает проект

При первом запуске:
1. создаёт SQLite-базу
2. создаёт демо-задачи
3. поднимает несколько воркеров
4. выполняет:
   - захват экрана
   - OCR указанного региона
   - health check

По умолчанию используется `mock_screen`, так что проект безопасно запускается без привязки к игре или чужому приложению.

## Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
python main.py