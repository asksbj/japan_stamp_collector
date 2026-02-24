from core.database import db_manager
from models.base import BaseModel


class JPostOffice(BaseModel):
    _table_name = "jpost_office"
    _columns = ["name", "address", "postcode", "latitude", "longtitude", "business_hours", "pref_id", "city_id"]
    _db_manager = db_manager

    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get("id")
        self.name = kwargs.get("name")
        self.address = kwargs.get("address")
        self.postcode = kwargs.get("postcode")
        self.latitude = kwargs.get("latitude")
        self.longtitude = kwargs.get("longtitude")
        self.business_hours = kwargs.get("business_hours")

        self.pref_id = kwargs.get("pref_id")
        self.city_id = kwargs.get("city_id")

    @classmethod
    def get_by_name_and_pref(cls, name: str, pref_id: int) -> "JPostOffice":
        if not name or not pref_id:
            return None

        query = f"SELECT * FROM {cls.get_table_name()} WHERE name = %s and pref_id = %s"
        params = (name, pref_id)

        return cls.get_db_results(query, params, fetch_one=True)


class Fuke(BaseModel):
    _table_name = "fuke"
    _columns = ["name", "abolition", "image_url", "start_date", "description", "author", "jpost_id"]
    _db_manager = db_manager

    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get("id")
        self.name = kwargs.get("name")
        self.abolition = kwargs.get("abolition")
        self.image_url = kwargs.get("image_url")
        self.start_date = kwargs.get("start_date")
        self.description = kwargs.get("description")
        self.author = kwargs.get("author")

        self.jpost_id = kwargs.get("jpost_id")

    @classmethod
    def get_by_name_and_jpost(cls, name: str, jpost_id: int) -> "Fuke":
        if not name or not jpost_id:
            return None

        query = f"SELECT * FROM {cls.get_table_name()} WHERE name = %s and jpost_id = %s"
        params = (name, jpost_id)

        return cls.get_db_results(query, params, fetch_one=True)