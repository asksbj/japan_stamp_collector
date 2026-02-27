from typing import List, Optional

from pydantic import BaseModel


class FukeItemOut(BaseModel):
    id: int
    name: str
    abolition: Optional[bool] = None
    image_url: Optional[str] = None
    start_date: Optional[str] = None
    description: Optional[str] = None
    author: Optional[str] = None

    jpost_office_name: str
    jpost_office_address: str
    jpost_office_postcode: str
    prefecture_name: str
    city_name: str


class FukeSearchResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: List[FukeItemOut]

