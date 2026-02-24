from core.database import db_manager
from models.base import BaseModel


class Prefecture(BaseModel):
    _table_name = "prefecture"
    _columns = ["name", "full_name", "en_name", "url", "pref_id"]
    _db_manager = db_manager

    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get('id')
        self.pref_id = kwargs.get("pref_id")
        self.name = kwargs.get('name')
        self.full_name = kwargs.get('full_name')
        self.en_name = kwargs.get('en_name')
        self.url = kwargs.get('url')

    def to_en_dict(self):
        return {
            self.en_name: {
                "id": self.id,
                "pref_id": self.pref_id,
                "full_name": self.full_name,
                "name": self.name,
                "url": self.url
            }
        }


class JPostOffice(BaseModel):
    _table_name = "jpost_office"
    _columns = ["name", "prefecture", "city", "address", "latitude", "longtitude", "business_hours"]
    _db_manager = db_manager

    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get('id')
        self.name = kwargs.get('office_name')
        self.prefecture = kwargs.get('prefecture')
        self.city = kwargs.get('city')
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


class Holiday(BaseModel):
    _table_name = "holiday"
    _columns = []
    _db_manager = db_manager

    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get('id')
        self.name = kwargs.get('name')
        self.date = kwargs.get('date')