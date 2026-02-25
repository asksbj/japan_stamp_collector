from typing import List, Optional
from fastapi import APIRouter, Query

from jpost.apis.models import CityOut, FukeItemOut, FukeSearchResponse, PrefectureOut
from jpost.models.administration import City, Prefecture
from jpost.models.jpost import Fuke


router = APIRouter(prefix="/api/fuke", tags=["fuke"])


@router.get("/prefectures", response_model=List[PrefectureOut])
def list_prefectures() -> List[PrefectureOut]:
    print("list_prefectures")
    prefectures = Prefecture.get_all()
    prefectures = sorted(prefectures, key=lambda p: p.pref_id or 0)
    return [
        PrefectureOut(
            id=p.id,
            name=p.name,
            full_name=p.full_name,
            en_name=p.en_name,
            pref_id=p.pref_id,
        )
        for p in prefectures
    ]


@router.get("/cities", response_model=List[CityOut])
def list_cities(pref_id: int = Query(..., gt=0)) -> List[CityOut]:
    cities = City.get_by_pref_id(pref_id)
    cities = sorted(cities, key=lambda c: c.id)
    return [
        CityOut(
            id=c.id,
            name=c.name,
            kind=c.kind,
            reading=c.reading,
            pref_id=c.pref_id,
        )
        for c in cities
    ]


@router.get("/search", response_model=FukeSearchResponse)
def search_fuke(
    pref_id: Optional[int] = Query(None, gt=0),
    city_id: Optional[int] = Query(None, gt=0),
    jpost_id: Optional[int] = Query(None, gt=0),
    abolition: Optional[bool] = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(12, ge=1, le=100),
) -> FukeSearchResponse:
    fuke_details = Fuke.get_fuke_details(pref_id=pref_id, city_id=city_id, jpost_id=jpost_id, abolition=abolition, page=page, page_size=page_size)

    total = len(fuke_details)

    if total == 0:
        return FukeSearchResponse(total=0, page=page, page_size=page_size, items=[])

    items: List[FukeItemOut] = []
    for f in fuke_details:
        f_id = f.get("id")
        name = f.get("name")
        abolition = f.get("abolition")
        image_url = f.get("image_url")
        start_date = f.get("start_date")
        description = f.get("description")
        author = f.get("author")
        jpost_office_name = f.get("jpost_office_name")
        jpost_office_address = f.get("jpost_office_address")
        jpost_office_postcode = f.get("jpost_office_postcode")
        prefecture_name = f.get("prefecture_name")
        city_name = f.get("city_name")
        items.append(
            FukeItemOut(
                id=f_id,
                name=name,
                abolition=abolition,
                image_url=image_url,
                start_date=start_date,
                description=description,
                author=author,
                jpost_office_name=jpost_office_name,
                jpost_office_address=jpost_office_address,
                jpost_office_postcode=jpost_office_postcode,
                prefecture_name=prefecture_name,
                city_name=city_name,
            )
        )

    return FukeSearchResponse(total=total, page=page, page_size=page_size, items=items)

