import datetime
import json
import logging
import shutil
import time
from pathlib import Path

import requests
from bs4 import BeautifulSoup

from core.settings import (
    MANHOLE_CARD_BASE_URL,
    TMP_ROOT,
    DEFAULT_REQUEST_DELAY,
    DEFAULT_TIMEOUT,
    REQUEST_DELAY_BEFORE_DOWNLOAD,
)
from etl.runner import TaskRunner
from models.administration import Prefecture


logging.basicConfig(level=logging.INFO)


class ManholeCardIngestor(TaskRunner):
    INTERVAL_DAYS = 7

    @classmethod
    def _load_prefectures(cls) -> dict:
        prefecture_dict: dict = {}
        prefectures = Prefecture.get_all()
        for prefecture in prefectures:
            prefecture_dict.update(prefecture.to_en_dict())
        return prefecture_dict

    @staticmethod
    def _fetch_html(url: str, timeout: int = DEFAULT_TIMEOUT) -> str:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text

    @staticmethod
    def _clean_location(td) -> str:
        if td is None:
            return ""
        parts = []
        for text in td.stripped_strings:
            s = str(text)
            lower = s.lower()
            if "電話" in s or "tel" in lower:
                break
            parts.append(s)
        return "\n".join(parts)

    @staticmethod
    def _extract_distribution_time(td) -> str:
        if td is None:
            return ""
        return "\n".join(list(td.stripped_strings))

    @staticmethod
    def _download_image(img_url: str, save_path: Path, timeout: int = DEFAULT_TIMEOUT) -> bool:
        try:
            resp = requests.get(img_url, timeout=timeout)
            resp.raise_for_status()
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "wb") as f:
                f.write(resp.content)
            return True
        except Exception as e:
            logging.error(f"Download image {img_url} failed: {e}")
            return False

    def _parse_table(self, html: str, images_dir: Path) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        table = soup.select_one("table.table1.cr")
        if not table:
            logging.info("No manhole card table found on page")
            return []

        records: list[dict] = []
        tbody = table.find("tbody") or table
        rows = tbody.find_all("tr")
        for tr in rows:
            tds = tr.find_all("td")
            if not tds:
                continue

            if len(tds) < 6:
                continue

            city_td = tds[0]
            img_td = tds[1]
            series_td = tds[2]
            release_date_td = tds[3]
            location_td = tds[4]
            distribution_time_td = tds[5]

            city = next(city_td.stripped_strings, '')
            series = series_td.get_text(strip=True)
            release_date = release_date_td.get_text(strip=True)

            location = self._clean_location(location_td)
            distribution_time = self._extract_distribution_time(distribution_time_td)

            img_tag = img_td.find("img")
            img_src = img_tag.get("src", "").strip() if img_tag else ""
            img_filename = ""
            if img_src:
                img_url = img_src
                img_filename = Path(img_url).name
                time.sleep(REQUEST_DELAY_BEFORE_DOWNLOAD)
                self._download_image(img_url, images_dir / img_filename)

            records.append(
                {
                    "city": city,
                    "series": series,
                    "release_date": release_date,
                    "location": location,
                    "distribution_time": distribution_time,
                    "image": img_filename,
                }
            )

        return records

    def _crawl_prefecture(self) -> int:
        key = self._task.owner
        prefecture = self._load_prefectures().get(key)
        if not prefecture:
            logging.error(f"Prefecture not found for key={key}")
            return self.FAILURE

        pref_id = prefecture.get("pref_id")
        if not pref_id:
            logging.error(f"Prefecture {key} has no pref_id")
            return self.FAILURE

        pref_id = str(pref_id) if pref_id >= 10 else "0" + str(pref_id)
        url = f"{MANHOLE_CARD_BASE_URL}?pref={pref_id}"
        logging.info(f"Requesting manhole card page for {key} (pref_id={pref_id})...")
        time.sleep(DEFAULT_REQUEST_DELAY)
        html = self._fetch_html(url)

        out_dir = TMP_ROOT / "manhole_card" / key
        images_dir = out_dir / "images"
        if out_dir.exists():
            shutil.rmtree(out_dir)
        images_dir.mkdir(parents=True, exist_ok=True)

        records = self._parse_table(html, images_dir)
        if not records:
            logging.info(f"No manhole card records parsed for {key}")
            return self.FAILURE

        data_path = out_dir / "data.json"
        with open(data_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        logging.info(f"Manhole card data saved for {key}, file path: {data_path}, total records: {len(records)}")
        return self.SUCCESS

    def start(self):
        result = self._crawl_prefecture()
        return result
