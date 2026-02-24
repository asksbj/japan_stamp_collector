from core.database import db_manager
from models.base import BaseModel


class Prefecture(BaseModel):
    _table_name = "prefecture"
    _columns = ["name", "full_name", "en_name", "jpost_url", "pref_id"]
    _db_manager = db_manager

    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get("id")
        self.name = kwargs.get("name")
        self.full_name = kwargs.get("full_name")
        self.en_name = kwargs.get("en_name")
        self.jpost_url = kwargs.get("jpost_url")
        self.pref_id = kwargs.get("pref_id")


    def to_en_dict(self):
        return {
            self.en_name: {
                "id": self.id,
                "pref_id": self.pref_id,
                "full_name": self.full_name,
                "name": self.name,
                "jpost_url": self.jpost_url
            }
        }


class City(BaseModel):
    _table_name = "city"
    _columns = ["name", "kind", "reading", "pref_id"]
    _db_manager = db_manager

    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get("id")
        self.name = kwargs.get("name")
        self.kind = kwargs.get("kind")
        self.reading = kwargs.get("reading")

        self.pref_id = kwargs.get("pref_id")

    @classmethod
    def get_by_name_and_pref(cls, name: str, pref_id: int) -> "City":
        if not name or not pref_id:
            return None

        query = f"SELECT * FROM {cls.get_table_name()} WHERE name = %s and pref_id = %s"
        params = (name, pref_id)

        return cls.get_db_results(query, params, fetch_one=True)


class Holiday(BaseModel):
    _table_name = "holiday"
    _columns = []
    _db_manager = db_manager

    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get("id")
        self.name = kwargs.get("name")
        self.date = kwargs.get("date")