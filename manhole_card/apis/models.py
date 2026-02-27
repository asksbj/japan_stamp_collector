from typing import List, Optional

from pydantic import BaseModel


class ManholeCardItemOut(BaseModel):
    id: int
    name: str
    series: str
    release_date: Optional[str] = None
    location_info: Optional[str] = None
    distribution_time: Optional[str] = None
    image_url: Optional[str] = None
    
    prefecture_name: str


class ManholeCardSearchResponse(BaseModel):
    total: int
    page: int
    page_size: int
    items: List[ManholeCardItemOut]