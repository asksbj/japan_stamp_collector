from typing import List, Optional
from fastapi import APIRouter, Query

from core.settings import MANHOLE_CARD_IMAGE_URL_PREFIX
from models.administration import Prefecture
from manhole_card.apis.models import ManholeCardItemOut, ManholeCardSearchResponse
from manhole_card.model import ManholeCard


router = APIRouter(prefix="/api/manhole-card", tags=["manhole_card"])


@router.get("/search", response_model=ManholeCardSearchResponse)
def search_manhole_card(
    pref_id: Optional[int] = Query(None, gt=0),
    name: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(12, ge=1, le=100),
) -> ManholeCardSearchResponse:

    manhole_cards, total = ManholeCard.get_by_pref_id_with_total(
        pref_id=pref_id,
        page=page,
        page_size=page_size,
    )
    if total == 0 or not manhole_cards:
        return ManholeCardSearchResponse(total=0, page=page, page_size=page_size, items=[])

    items: List[ManholeCardItemOut] = []
    for mc in manhole_cards:
        mc_id = mc.get("id")
        name = mc.get("name")
        series = mc.get("series")
        release_date = mc.get("release_date")
        location_info = mc.get("location_info")
        distribution_time = mc.get("distribution_time")
        prefecture_name = mc.get("prefecture_name")
        pref_en = mc.get("prefecture_en") or ""


        raw_image = mc.get("image_url") or ""
        if raw_image.startswith("http://") or raw_image.startswith("https://"):
            image_url = raw_image
        elif raw_image and pref_en:
            image_url = f"{MANHOLE_CARD_IMAGE_URL_PREFIX.rstrip('/')}/{pref_en}/images/{raw_image.lstrip('/')}"
        else:
            image_url = None

        items.append(
            ManholeCardItemOut(
                id=mc_id,
                name=name,
                series=series,
                release_date=release_date,
                location_info=location_info,
                distribution_time=distribution_time,
                image_url=image_url,
                prefecture_name=prefecture_name,
            )
        )

    return ManholeCardSearchResponse(total=total, page=page, page_size=page_size, items=items)

