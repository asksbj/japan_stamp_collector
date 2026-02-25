from typing import List, Optional
from fastapi import APIRouter, HTTPException, Query

from core.database import db_manager
from jpost.apis.models import CityOut, FukeItemOut, FukeSearchResponse, PrefectureOut
from jpost.models.administration import City, Prefecture


router = APIRouter(prefix="/api/fuke", tags=["fuke"])


@router.get("/prefectures", response_model=List[PrefectureOut])
def list_prefectures() -> List[PrefectureOut]:
    prefectures = Prefecture.get_all()
    prefectures = sorted(prefectures, key=lambda p: p.pref_id or 0)
    return [
        PrefectureOut(
            id=p.id,
            name=p.name,
            full_name=p.full_name,
            en_name=p.en_name,
            pref_id=p.pref_id,
        )
        for p in prefectures
    ]


@router.get("/cities", response_model=List[CityOut])
def list_cities(pref_id: int = Query(..., gt=0)) -> List[CityOut]:
    query = f"SELECT * FROM {City.get_table_name()} WHERE pref_id = %s ORDER BY name"
    rows = City.get_db_manager().execute_query(query, (pref_id,), fetch_all=True)
    if not rows:
        return []

    cities: List[CityOut] = []
    for row in rows:
        obj = City.from_db(row)
        cities.append(
            CityOut(
                id=obj.id,
                name=obj.name,
                kind=obj.kind,
                reading=obj.reading,
                pref_id=obj.pref_id,
            )
        )
    return cities


@router.get("/search", response_model=FukeSearchResponse)
def search_fuke(
    pref_id: Optional[int] = Query(None, gt=0),
    city_id: Optional[int] = Query(None, gt=0),
    page: int = Query(1, ge=1),
    page_size: int = Query(12, ge=1, le=100),
) -> FukeSearchResponse:
    offset = (page - 1) * page_size

    base_join = """
        FROM fuke f
        JOIN jpost_office o ON f.jpost_id = o.id
        JOIN city c ON o.city_id = c.id
        JOIN prefecture p ON o.pref_id = p.id
        WHERE 1=1
    """

    conditions = []
    params: list = []

    if pref_id is not None:
        conditions.append("AND p.id = %s")
        params.append(pref_id)

    if city_id is not None:
        conditions.append("AND c.id = %s")
        params.append(city_id)

    where_clause = " ".join(conditions)

    count_sql = f"SELECT COUNT(*) {base_join} {where_clause}"
    count_row = db_manager.execute_query(count_sql, tuple(params), fetch_one=True)
    total = count_row[0] if count_row else 0

    if total == 0:
        return FukeSearchResponse(total=0, page=page, page_size=page_size, items=[])

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
            o.pref_id,
            o.city_id
        {base_join}
        {where_clause}
        ORDER BY f.start_date IS NULL, f.start_date, f.id
        LIMIT %s OFFSET %s
    """

    data_params = list(params)
    data_params.extend([page_size, offset])

    rows = db_manager.execute_query(data_sql, tuple(data_params), fetch_all=True)
    if rows is None:
        raise HTTPException(status_code=500, detail="Failed to fetch fuke data")

    items: List[FukeItemOut] = []
    for (
        f_id,
        name,
        abolition,
        image_url,
        start_date,
        description,
        author,
        jpost_office_name,
        jpost_office_address,
        jpost_office_postcode,
        row_pref_id,
        row_city_id,
    ) in rows:
        items.append(
            FukeItemOut(
                id=f_id,
                name=name,
                abolition=abolition,
                image_url=image_url,
                start_date=start_date,
                description=description,
                author=author,
                jpost_office_name=jpost_office_name,
                jpost_office_address=jpost_office_address,
                jpost_office_postcode=jpost_office_postcode,
                pref_id=row_pref_id,
                city_id=row_city_id,
            )
        )

    return FukeSearchResponse(total=total, page=page, page_size=page_size, items=items)

