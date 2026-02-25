from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from core.settings import APP_NAME_EN, APP_NAME_JA, APP_NAME_ZH, STATIC_ROOT, TEMPLATES_ROOT
from jpost.apis.fuke import router as fuke_router


app = FastAPI(title=f"{APP_NAME_EN}")

static_dir = STATIC_ROOT
templates_dir = TEMPLATES_ROOT

app.mount("/static", StaticFiles(directory=static_dir), name="static")
templates = Jinja2Templates(directory=str(templates_dir))


@app.get("/", response_class=HTMLResponse)
async def read_home(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "app_name_en": APP_NAME_EN,
            "app_name_ja": APP_NAME_JA,
            "app_name_zh": APP_NAME_ZH,
        },
    )


@app.get("/fuke", response_class=HTMLResponse)
async def fuke_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "fuke.html",    
        {
            "request": request,
            "app_name_en": APP_NAME_EN,
            "app_name_zh": APP_NAME_ZH,
        },
    )


app.include_router(fuke_router)

