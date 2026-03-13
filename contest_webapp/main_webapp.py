from contextlib import asynccontextmanager
import os
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from contest_api import router as contest_router
from contest_api import shutdown, startup

BASE_DIR = Path(__file__).resolve().parent
APP_ASSET_VERSION = os.getenv("CONTEST_WEBAPP_ASSET_VERSION", "20260313-6")


@asynccontextmanager
async def lifespan(_: FastAPI):
    await startup()
    try:
        yield
    finally:
        await shutdown()


app = FastAPI(lifespan=lifespan)
app.include_router(contest_router, prefix="/api")
app.mount("/static", StaticFiles(directory=BASE_DIR), name="static")


@app.get("/")
async def root() -> HTMLResponse:
    html = (BASE_DIR / "index.html").read_text(encoding="utf-8")
    html = html.replace("__APP_ASSET_VERSION__", APP_ASSET_VERSION)
    return HTMLResponse(
        content=html,
        headers={
            "Cache-Control": "no-store, no-cache, must-revalidate, max-age=0",
            "Pragma": "no-cache",
            "Expires": "0",
        },
    )