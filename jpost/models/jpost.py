from typing import Optional, List
from xxlimited import Str
from core.database import db_manager
from models.base import BaseModel


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
    def _build_where_clause(
        cls,
        pref_id: Optional[int] = None,
        city_id: Optional[int] = None,
        jpost_name: Optional[str] = None,
        abolition: Optional[bool] = None,
    ) -> tuple[str, list]:
        conditions = []
        params: list = []

        if pref_id is not None:
            conditions.append("AND p.pref_id = %s")
            params.append(pref_id)

        if city_id is not None:
            conditions.append("AND c.id = %s")
            params.append(city_id)

        if jpost_name is not None:
            conditions.append("AND o.name = %s")
            params.append(jpost_name)

        if abolition is not None:
            conditions.append("AND f.abolition = %s")
            params.append(int(abolition))

        where_clause = " ".join(conditions)
        return where_clause, params

    @classmethod
    def get_fuke_details(
        cls, 
        pref_id: Optional[int] = None, 
        city_id: Optional[int] = None,
        jpost_name: Optional[str] = None,
        abolition: Optional[bool] = None,
        page: int = 1,
        page_size: int = 12,
    ) -> List[dict]:
        offset = (page - 1) * page_size

        where_clause, params = cls._build_where_clause(
            pref_id=pref_id,
            city_id=city_id,
            jpost_name=jpost_name,
            abolition=abolition,
        )

        base_join = """
            FROM fuke f
            JOIN facility o ON f.jpost_id = o.id
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
                p.full_name AS prefecture_name,
                p.en_name AS prefecture_en
            {base_join}
            {where_clause}
            ORDER BY f.abolition, c.id IS NULL, c.id, f.id
            LIMIT %s OFFSET %s
        """

        columns = [
            "id",
            "name",
            "abolition",
            "image_url",
            "start_date",
            "description",
            "author",
            "jpost_office_name",
            "jpost_office_address",
            "jpost_office_postcode",
            "city_name",
            "prefecture_name",
            "prefecture_en",
        ]
        data_params = list(params)
        data_params.extend([page_size, offset])

        rows = cls.get_db_manager().execute_query(data_sql, tuple(data_params), fetch_all=True)
        return [dict(zip(columns, row)) for row in rows] if rows else []

    @classmethod
    def get_fuke_details_with_total(
        cls,
        pref_id: Optional[int] = None,
        city_id: Optional[int] = None,
        jpost_name: Optional[str] = None,
        abolition: Optional[bool] = None,
        page: int = 1,
        page_size: int = 12,
    ) -> tuple[List[dict], int]:
        where_clause, params = cls._build_where_clause(
            pref_id=pref_id,
            city_id=city_id,
            jpost_name=jpost_name,
            abolition=abolition,
        )

        base_join = """
            FROM fuke f
            JOIN facility o ON f.jpost_id = o.id
            LEFT JOIN city c ON o.city_id = c.id
            JOIN prefecture p ON o.pref_id = p.pref_id
            WHERE 1=1
        """

        count_sql = f"SELECT COUNT(*) {base_join} {where_clause}"
        count_row = cls.get_db_manager().execute_query(count_sql, tuple(params), fetch_one=True)
        total = int(count_row[0]) if count_row else 0

        if total == 0:
            return [], 0

        items = cls.get_fuke_details(
            pref_id=pref_id,
            city_id=city_id,
            jpost_name=jpost_name,
            abolition=abolition,
            page=page,
            page_size=page_size,
        )
        return items, total