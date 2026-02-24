from core.database import db_manager
from models.base import BaseModel


class JPostOffice(BaseModel):
    _table_name = "jpost_office"
    _columns = ["name", "prefecture_id", "city_id", "address", "latitude", "longtitude", "business_hours"]
    _db_manager = db_manager

    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get('id')
        self.name = kwargs.get('office_name')
        self.prefecture_id = kwargs.get('prefecture_id')
        self.city_id = kwargs.get('city')
        self.address = kwargs.get('office_location')
        self.latitude = kwargs.get('latitude')
        self.longtitude = kwargs.get('longtitude')
        self.business_hours = kwargs.get('business_hours')


class Fuke(BaseModel):
    _table_name = "fuke"
    _columns = []
    _db_manager = db_manager

    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get('id')
        self.fuke_name = kwargs.get('fuke_name')
        self.abolition = kwargs.get('abolition')
        self.image_url = kwargs.get('image_url')
        self.detailed_url = kwargs.get('detailed_url')
        self.start_date = kwargs.get('start_date')
        self.description = kwargs.get('description')
        self.author = kwargs.get('author')

        self.post_office_id = kwargs.get('author')