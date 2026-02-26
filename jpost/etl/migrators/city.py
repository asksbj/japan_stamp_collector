import logging
import json
import pykakasi

from core.settings import PROJECT_ROOT
from etl.runner import TaskRunner
from jpost.models.administration import Prefecture, City

logging.basicConfig(level=logging.INFO)


class CityMigrator(TaskRunner):
    INTERVAL_DAYS = 7

    @classmethod
    def _load_prefectures(cls) -> dict[str, Prefecture]:
        prefectures = Prefecture.get_all()
        return {p.en_name: p for p in prefectures}

    @classmethod
    def _parse_reading(cls, name: str) -> str:
        kks = pykakasi.kakasi()
        result = kks.convert(name)
        roman = ''.join([item['hepburn'] for item in result]).capitalize()
        return roman

    def start(self):
        dist_dir = PROJECT_ROOT / "dist"
        city_path = dist_dir / "city.json"
        try:
            with open(city_path, "r", encoding="utf-8") as f:
                records = json.load(f)
        except FileNotFoundError as e:
            logging.error(f"Data file {city_path} can not be found.")
            return self.FAILURE

        prefectures = self._load_prefectures()
        for p, city_list in records.items():
            prefecture = prefectures.get(p)
            if not prefecture:
                continue
            pref_id = prefecture.pref_id

            new_cities = []
            for city_dict in city_list:
                name = city_dict.get("name")
                city = City.get_by_name_and_pref(name, pref_id)
                if city:
                    continue

                city_dict["pref_id"] = pref_id
                city_dict["reading"] = self._parse_reading(name)
                new_cities.append(City(**city_dict))

            if new_cities:
                City.bulk_insert(new_cities)

        return self.SUCCESS

        