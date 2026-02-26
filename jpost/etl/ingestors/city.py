import json
import logging
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from core.settings import (
    PROJECT_ROOT,
    JAPAN_CITY_BASE_URL,
    DEFAULT_TIMEOUT,
    DEFAULT_REQUEST_DELAY,
)
from etl.runner import TaskRunner
from models.administration import Prefecture


logging.basicConfig(level=logging.INFO)


class CityIngestor(TaskRunner):
    TASK_TIMEOUT_SECS = 24*60*60
    INTERVAL_DAYS = 7

    @classmethod
    def _load_prefectures(cls) -> dict[str, Prefecture]:
        prefectures = Prefecture.get_all()
        return {p.en_name: p for p in prefectures}

    @classmethod
    def _fetch_html(cls, url: str, timeout: int = DEFAULT_TIMEOUT) -> str:
        resp = requests.get(url, headers={}, timeout=timeout)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text

    @classmethod
    def _parse_prefecture(cls, html: str, is_tokyo: bool = False) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")

        target_table = None
        for table in soup.find_all("table"):
            header = table.find("tr")
            if not header:
                continue
            header_cells = [
                c.get_text(strip=True) for c in header.find_all(["th", "td"])
            ]
            if header_cells and "都道府県" in header_cells[0]:
                target_table = table
                break

        if not target_table:
            logging.warning("No target table found when parsing prefecture HTML")
            return []

        records: list[dict] = []
        section = None

        def parse_int(s: str):
            if s is None:
                return None
            s = s.replace(",", "").strip()
            return int(s) if s.isdigit() else None

        def parse_float(s: str):
            try:
                return float(s.replace(",", "").strip())
            except (ValueError, AttributeError):
                return None

        for tr in target_table.find_all("tr"):
            cells = [c.get_text(strip=True) for c in tr.find_all(["th", "td"])]
            if not cells:
                continue

            first = cells[0]

            # Section headers
            if first == "市" and "読み" in cells:
                section = "city"
                continue
            if first == "特別区" and "読み" in cells:
                section = "tokyo_ward"
                continue
            if first == "町村" and "読み" in cells:
                section = "town"
                continue
            if first == "都道府県" and "読み" in cells:
                section = "pref_header"
                continue

            # Skip summary / meta rows
            if first in ("市部合計", "郡部合計"):
                continue
            if first.startswith("※"):
                continue
            if first.endswith("振興局") or first.endswith("総合振興局") or first.endswith("支庁"):
                continue

            name = first
            kind = None

            # 郡: counties, regardless of section
            if name.endswith("郡"):
                kind = "county"

            # 一般の市
            if section == "city" and name.endswith("市"):
                kind = kind or "city"

            # 東京の特別区（23区）
            if is_tokyo and section == "tokyo_ward":
                if name == "特別区合計":
                    continue
                if name.endswith("区"):
                    kind = "ward"

            # 札幌市などの区はスキップ（東京以外）
            if not is_tokyo and name.endswith("区"):
                continue

            if not kind:
                continue

            reading = cells[1] if len(cells) > 1 else ""

            records.append(
                {
                    "name": name,
                    "kind": kind,
                    "reading": reading,
                }
            )

        return records

    def start(self):
        prefectures_by_en = self._load_prefectures()
        all_data: dict[str, list[dict]] = {}

        for en_name, pref in prefectures_by_en.items():
            slug = en_name.lower()
            url = f"{JAPAN_CITY_BASE_URL}{slug}.html"
            logging.info(f"Fetching city information for {pref.full_name} from {url}")
            time.sleep(DEFAULT_REQUEST_DELAY)

            try:
                html = self._fetch_html(url)
            except Exception as e:
                logging.error(
                    f"Failed to fetch city data for {pref.full_name} ({en_name}) from {url}: {e}"
                )
                continue

            is_tokyo = slug == "tokyo"
            records = self._parse_prefecture(html, is_tokyo=is_tokyo)
            all_data[en_name] = records

        dist_dir = PROJECT_ROOT / "dist"
        dist_dir.mkdir(parents=True, exist_ok=True)
        city_path = dist_dir / "city.json"
        with open(city_path, "w", encoding="utf-8") as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)

        logging.info(f"City data saved to {city_path}")
        return self.SUCCESS