from typing import List, Optional

from core.database import db_manager
from models.administration import Facility
from models.base import BaseModel


class ManholeCard(BaseModel):
    _table_name = "manhole_card"
    _columns = ["name", "series", "release_date", "location_info", "distribution_time", "image_url"]
    _db_manager = db_manager

    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get("id")
        self.name = kwargs.get("name")
        self.series = kwargs.get("series")
        self.release_date = kwargs.get("release_date")
        self.location_info = kwargs.get("location_info")
        self.distribution_time = kwargs.get("distribution_time")
        self.image_url = kwargs.get("image_url")

    def location_detail(self) -> List[dict]:
        facilities = ManholeCardFacility.get_facilities(self.id)

        result = []
        for facility in facilities:
            result.append(facility.to_dict())
        return result

    @classmethod
    def get_by_name_and_series(cls, name: str, series: str) -> "ManholeCard":
        if not name or not series:
            return None

        query = (
            f"SELECT * FROM {cls.get_table_name()} "
            "WHERE name = %s AND series = %s"
        )
        params = (name, series)

        return cls.get_db_results(query, params, fetch_one=True)


class ManholeCardFacility(BaseModel):
    _table_name = "manhole_card_facility"
    _columns = ["manhole_card_id", "facility_id"]
    _db_manager = db_manager

    def __init__(self, **kwargs) -> None:
        self.id = kwargs.get("id")
        self.manhole_card_id = kwargs.get("manhole_card_id")
        self.facility_id = kwargs.get("facility_id")

    @classmethod
    def get_by_fuzzy_id(cls, manhole_card_id: Optional[int] = None, facility_id: Optional[int] = None) -> List["ManholeCardFacility"]:
        if not manhole_card_id and not facility_id:
            return None

        conditions = []
        params = []
        if manhole_card_id is not None:
            conditions.append("AND manhole_card_id = %s")
            params.append(manhole_card_id)

        if facility_id is not None:
            conditions.append("AND facility_id = %s")
            params.append(facility_id)

        where_clause = " ".join(conditions)
        query = f"SELECT * FROM {cls.get_table_name()} WHERE 1=1 {where_clause}"
        return cls.get_db_results(query, tuple(params))

    @classmethod
    def get_facilities(cls, manhole_card_id: int) -> List["Facility"]:
        if not manhole_card_id:
            return []

        query = f"SELECT facility_id FROM {cls.get_table_name()} WHERE manhole_card_id = %s"
        params = (manhole_card_id,)
        rows = cls.get_db_manager().execute_query(query, params, fetch_all=True)
        if not rows:
            return []

        facility_ids = [row[0] for row in rows]
        if not facility_ids:
            return []
        facility_ids = list(set(facility_ids))

        placeholders = ", ".join(["%s"] * len(facility_ids))
        facility_query = f"SELECT * FROM {Facility.get_table_name()} WHERE id IN ({placeholders})"
        facilities = Facility.get_db_results(facility_query, tuple(facility_ids))
        return facilities