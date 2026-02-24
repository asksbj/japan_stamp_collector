import json
import logging
from decimal import Decimal
from pathlib import Path

from core.settings import TMP_ROOT
from jpost.etl.migrators.base import BaseMigrator
from jpost.models.administration import Prefecture, City
from jpost.models.jpost import JPostOffice, Fuke


logging.basicConfig(level=logging.INFO)


class FukeMigrator(BaseMigrator):
    MIGRATE_INTERVAL_DAYS = 1
    DESCRIPTION_MAX_LENTH = 250
    AUTHOR_MAX_LENTH = 28

    @classmethod
    def _load_prefectures(cls) -> dict[str, Prefecture]:
        prefectures = Prefecture.get_all()
        return {p.en_name: p for p in prefectures}

    @classmethod
    def _load_cities_by_pref(cls) -> dict[int, list[tuple[str, int]]]:
        cities = City.get_all()
        cities_by_pref: dict[int, list[tuple[str, int]]] = {}
        for c in cities:
            if not c.pref_id or not c.name:
                continue
            cities_by_pref.setdefault(c.pref_id, []).append((c.name, c.id))

        for pref_id, city_list in cities_by_pref.items():
            city_list.sort(key=lambda item: len(item[0]), reverse=True)

        return cities_by_pref

    @staticmethod
    def _parse_address_from_location(location: str) -> str:
        if not location:
            return ""
        parts = location.split("\n")
        if len(parts) >= 2:
            return parts[1].strip()
        return location.strip()

    @staticmethod
    def _detect_city_id_from_location(
        location: str, pref_id: int, cities_by_pref: dict[int, list[tuple[str, int]]]
    ) -> int | None:
        if not location or not pref_id:
            return None

        second_line = location.split("\n")[1]
        candidates = cities_by_pref.get(pref_id) or []
        for name, city_id in candidates:
            if name and name in second_line:
                return city_id
        return None

    @staticmethod
    def _parse_geo_from_address(address_obj) -> tuple[Decimal | None, Decimal | None, str | None]:
        if not isinstance(address_obj, dict):
            return None, None, None

        lat_raw = address_obj.get("lat")
        long_raw = address_obj.get("long")
        postcode = address_obj.get("postcode")

        lat_val: Decimal | None = None
        long_val: Decimal | None = None

        if lat_raw is not None:
            try:
                lat_val = Decimal(str(lat_raw))
            except Exception:
                lat_val = None

        if long_raw is not None:
            try:
                long_val = Decimal(str(long_raw))
            except Exception:
                long_val = None

        return lat_val, long_val, postcode

    def _upsert_jpost_office(
        self,
        record: dict,
        pref_id: int,
        cities_by_pref: dict[int, list[tuple[str, int]]],
    ) -> JPostOffice | None:
        jpost_name = (record.get("post_office_name") or "").strip()
        if not jpost_name:
            return None

        location = record.get("location") or ""
        address_obj = record.get("address")  # geo info dict

        address = self._parse_address_from_location(location)
        latitude, longtitude, postcode = self._parse_geo_from_address(address_obj)
        city_id = self._detect_city_id_from_location(location, pref_id, cities_by_pref)

        existing = JPostOffice.get_by_name_and_pref(jpost_name, pref_id)
        if existing:
            jpost = existing
        else:
            jpost = JPostOffice(name=jpost_name, pref_id=pref_id)

        jpost.address = address
        jpost.postcode = postcode
        jpost.latitude = latitude
        jpost.longtitude = longtitude
        jpost.business_hours = None
        jpost.pref_id = pref_id
        jpost.city_id = city_id

        success = jpost.save()
        if not success:
            logging.error(f"Failed to save JPostOffice for {jpost_name}")
            return None
        return jpost

    def _upsert_fuke(self, record: dict, jpost_id: int) -> Fuke | None:
        if not jpost_id:
            return None

        fuke_name = (record.get("fuke_name") or "").strip()
        if not fuke_name:
            return None

        abolition = bool(record.get("abolition"))
        image_url = record.get("image") or ""
        start_date = record.get("date") or ""
        description = record.get("description") or ""
        author = record.get("author") or ""
        if len(description) > self.DESCRIPTION_MAX_LENTH:
            description = description[:self.DESCRIPTION_MAX_LENTH] + "..."
        if len(author) > self.AUTHOR_MAX_LENTH:
            author = author[:self.AUTHOR_MAX_LENTH] + "..."

        existing = Fuke.get_by_name_and_jpost(fuke_name, jpost_id, abolition=abolition)
        if existing:
            fuke = existing
        else:
            fuke = Fuke(name=fuke_name, jpost_id=jpost_id)

        fuke.abolition = abolition
        fuke.image_url = image_url
        fuke.start_date = start_date
        fuke.description = description
        fuke.author = author
        fuke.jpost_id = jpost_id

        success = fuke.save()
        if not success:
            logging.error(f"Failed to save Fuke for {fuke_name} (jpost_id={jpost_id})")
            return None
        return fuke

    def migrate(self):
        tmp_root = TMP_ROOT
        if not tmp_root.exists():
            logging.error(f"TMP_ROOT directory not found: {tmp_root}")
            return self.FAILURE

        prefectures = self._load_prefectures()
        cities_by_pref = self._load_cities_by_pref()

        changed = False

        for pref_dir in tmp_root.iterdir():
            if not pref_dir.is_dir():
                continue

            data_file = Path(pref_dir) / "data.json"
            if not data_file.exists():
                continue

            key = pref_dir.name
            prefecture = prefectures.get(key)
            if not prefecture:
                logging.warning(f"Skip directory {key}: prefecture not found in DB")
                continue

            pref_id = prefecture.pref_id

            try:
                with open(data_file, "r", encoding="utf-8") as f:
                    records = json.load(f)
            except (OSError, json.JSONDecodeError) as e:
                logging.error(f"Failed to load data from {data_file}: {e}")
                continue

            logging.info(f"Migrating Fuke data for prefecture {key} from {data_file}")

            for r in records:
                jpost = self._upsert_jpost_office(r, pref_id, cities_by_pref)
                if not jpost or not jpost.id:
                    continue

                fuke = self._upsert_fuke(r, jpost.id)
                if jpost or fuke:
                    changed = True

        if changed:
            return self.SUCCESS
        else:
            return self.NO_WORK_TO_DO

