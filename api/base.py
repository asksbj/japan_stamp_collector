from typing import List
from fastapi import APIRouter, Query

from api.models import PrefectureOut, CityOut
from models.administration import City, Prefecture


router = APIRouter(prefix="/api", tags=["base"])


@router.get("/prefectures", response_model=List[PrefectureOut])
def list_prefectures() -> List[PrefectureOut]:
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