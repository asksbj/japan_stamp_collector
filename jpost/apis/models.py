from datetime import date
from typing import List, Optional

from pydantic import BaseModel


class PrefectureOut(BaseModel):
    id: int
    name: str
    full_name: str
    en_name: str
    pref_id: int


class CityOut(BaseModel):
    id: int
    name: str
    kind: Optional[str] = None
    reading: Optional[str] = None
    pref_id: int


class FukeItemOut(BaseModel):
    id: int
    name: str
    abolition: Optional[bool] = None
    image_url: Optional[str] = None
    start_date: Optional[date] = None
    description: Optional[str] = None
    author: Optional[str] = None

    jpost_office_name: str
    jpost_office_address: str
    jpost_office_postcode: str
    pref_id: int
    city_id: int


class FukeSearchResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: List[FukeItemOut]

