from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from core.settings import (
    APP_NAME_EN,
    APP_NAME_JA,
    APP_NAME_ZH,
    STATIC_ROOT,
    TEMPLATES_ROOT,
    FUKE_IMAGE_ROOT,
    FUKE_IMAGE_URL_PREFIX,
    FUKE_IMAGE_ENABLE_LOCAL,
    MANHOLE_CARD_IMAGE_ROOT,
    MANHOLE_CARD_IMAGE_URL_PREFIX,
    MANHOLE_CARD_IMAGE_ENABLE_LOCAL
)
from api.base import router as base_router
from jpost.apis.fuke import router as fuke_router
from manhole_card.apis.manhole_card import router as manhole_card_router


app = FastAPI(title=f"{APP_NAME_EN}")

static_dir = STATIC_ROOT
templates_dir = TEMPLATES_ROOT

app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Serve local Fuke images from TMP_ROOT (or configured FUKE_IMAGE_ROOT).
if FUKE_IMAGE_ENABLE_LOCAL and FUKE_IMAGE_ROOT.exists():
    app.mount(
        FUKE_IMAGE_URL_PREFIX,
        StaticFiles(directory=FUKE_IMAGE_ROOT),
        name="fuke-images",
    )

if MANHOLE_CARD_IMAGE_ENABLE_LOCAL and MANHOLE_CARD_IMAGE_ROOT.exists():
    app.mount(
        MANHOLE_CARD_IMAGE_URL_PREFIX,
        StaticFiles(directory=MANHOLE_CARD_IMAGE_ROOT),
        name="manhole-card-images",
    )

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


@app.get("/manhole_card", response_class=HTMLResponse)
async def manhole_card_page(request: Request) -> HTMLResponse:
    return templates.TemplateResponse(
        "manhole_card.html",    
        {
            "request": request,
            "app_name_en": APP_NAME_EN,
            "app_name_zh": APP_NAME_ZH,
        },
    )

app.include_router(base_router)
app.include_router(fuke_router)
app.include_router(manhole_card_router)
