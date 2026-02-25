from typing import Optional, List
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
    def get_by_name_and_jpost(cls, name: str, jpost_id: int, abolition: bool | None = None) -> "Fuke":
        if not name or not jpost_id:
            return None

        if abolition is not None:
            query = f"SELECT * FROM {cls.get_table_name()} WHERE name = %s and jpost_id = %s and abolition = %s"
            params = (name, jpost_id, abolition)
        else:
            query = f"SELECT * FROM {cls.get_table_name()} WHERE name = %s and jpost_id = %s"
            params = (name, jpost_id)

        return cls.get_db_results(query, params, fetch_one=True)

    @classmethod
    def get_fuke_details(
        cls, 
        pref_id: Optional[int] = None, 
        city_id: Optional[int] = None,
        jpost_id: Optional[int] = None,
        abolition: Optional[bool] = None,
        page: int = 1,
        page_size: int = 12,
    ) -> List[dict]:
        offset = (page - 1) * page_size

        conditions = []
        params: list = []

        if pref_id is not None:
            conditions.append("AND p.pref_id = %s")
            params.append(pref_id)

        if city_id is not None:
            conditions.append("AND c.id = %s")
            params.append(city_id)

        if jpost_id is not None:
            conditions.append("AND f.jpost_id = %s")
            params.append(jpost_id)

        if abolition is not None:
            conditions.append("AND f.abolition = %s")
            params.append(int(abolition))

        where_clause = " ".join(conditions)

        base_join = """
            FROM fuke f
            JOIN jpost_office o ON f.jpost_id = o.id
            LEFT JOIN city c ON o.city_id = c.id
            JOIN prefecture p ON o.pref_id = p.pref_id
            WHERE 1=1
        """

        data_sql = f"""
            SELECT
                f.id,
                f.name,
                f.abolition,
                f.image_url,
                f.start_date,
                f.description,
                f.author,
                o.name AS jpost_office_name,
                o.address AS jpost_office_address,
                o.postcode AS jpost_office_postcode,
                COALESCE(c.name, '') AS city_name,
                p.full_name AS prefecture_name
            {base_join}
            {where_clause}
            ORDER BY f.start_date IS NULL, f.start_date, f.id
            LIMIT %s OFFSET %s
        """

        columns = ["id", "name", "abolition", "image_url", "start_date", "description", "author", "jpost_office_name", "jpost_office_address", "jpost_office_postcode", "city_name", "prefecture_name"]
        data_params = list(params)
        data_params.extend([page_size, offset])

        rows = cls.get_db_manager().execute_query(data_sql, tuple(data_params), fetch_all=True)
        return [dict(zip(columns, row)) for row in rows] if rows else []