from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from contest_api import router as contest_router
from contest_api import shutdown, startup

BASE_DIR = Path(__file__).resolve().parent


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
async def root() -> FileResponse:
    return FileResponse(BASE_DIR / "index.html")