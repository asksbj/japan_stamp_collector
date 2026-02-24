import logging
import json

from core.settings import PROJECT_ROOT
from jpost.etl.migrators.base import BaseMigrator
from jpost.models.administration import Prefecture, City

logging.basicConfig(level=logging.INFO)


class CityMigrator(BaseMigrator):
    MIGRATE_INTERVAL_DAYS = 7

    @classmethod
    def _load_prefectures(cls) -> dict[str, Prefecture]:
        prefectures = Prefecture.get_all()
        return {p.en_name: p for p in prefectures}

    def migrate(self):
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
                new_cities.append(City(**city_dict))

            if new_cities:
                City.bulk_insert(new_cities)

        return self.SUCCESS

        