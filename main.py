from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from jpost.apis.fuke import router as fuke_router


BASE_DIR = Path(__file__).resolve().parent

app = FastAPI(title="Japan Stamp Collector / 日本印章・卡片收集者")

static_dir = BASE_DIR / "static"
templates_dir = BASE_DIR / "templates"

app.mount("/static", StaticFiles(directory=static_dir), name="static")
templates = Jinja2Templates(directory=str(templates_dir))


@app.get("/", response_class=HTMLResponse)
async def read_home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "app_name_en": "Japan Stamp Collector",
            "app_name_ja": "日本スタンプ・カードコレクター",
            "app_name_zh": "日本印章・卡片收集者",
        },
    )


@app.get("/fuke", response_class=HTMLResponse)
async def fuke_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "fuke.html",
        {
            "request": request,
            "app_name_en": "Japan Stamp Collector",
            "app_name_zh": "日本印章・卡片收集者",
        },
    )


app.include_router(fuke_router)

