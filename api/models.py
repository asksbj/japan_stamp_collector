from typing import Optional
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