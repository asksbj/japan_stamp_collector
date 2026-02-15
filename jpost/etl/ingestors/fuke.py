import datetime
import logging
import json
import re
import requests
import shutil
import time
from bs4 import BeautifulSoup
from pathlib import Path
from urllib3.poolmanager import key_fn_by_scheme

from core.settings import (
    TMP_ROOT, JPOST_BASE_URL, 
    FUKE_BASE_URL, FUKE_HEADERS, 
    DEFAULT_JPOST_REQUEST_TIMEOUT, 
    DEFAULT_REQUEST_DELAY, 
    REQUEST_DELAY_BEFORE_DOWNLOAD
)
from jpost.enums.text import JPTextEnum
from jpost.etl.ingestors.base import BaseIngestor
from jpost.models.jpost import Prefecture
from jpost.models.ingestor import FukeIngestorRecords


logging.basicConfig(level=logging.INFO)


class FukeIngestorMixin(object):

    @classmethod
    def _load_prefectures(cls):
        prefecture_dict = {}

        prefectures = Prefecture.get_all()
        for prefecture in prefectures:
            prefecture_dict.update(prefecture.to_en_dict())
        
        return prefecture_dict
        

class FukeBasicIngestor(FukeIngestorMixin, BaseIngestor):
    TASK_TIMEOUT_SECS = 900

    @classmethod
    def _parse_stamp_posts(cls, html: str) -> list[dict]:
        soup = BeautifulSoup(html, "html.parser")
        posts = soup.select("div.post")
        stamps = []

        for post in posts:
            date_span = post.select_one("span.date")
            date_val = (date_span.get_text(strip=True).replace("\xa0", "") if date_span else "").strip()

            link_a = post.select_one("span.link a[href*='detail.php']")
            detail_href = link_a.get("href", "") if link_a else ""
            detail_id_match = re.search(r"id=(\d+)", detail_href)
            detail_id = detail_id_match.group(1) if detail_id_match else None
            if not detail_id:
                continue

            img = post.select_one("dt img")
            img_src = img.get("src", "") if img else ""
            fuke_name = (img.get("alt", "") if img else "").strip()

            title_dd = post.select_one("dd.title")
            post_office_name = (title_dd.get_text(strip=True) if title_dd else "").strip()

            abolition_dd = post.select_one("dd.abolition")
            abolition = bool(abolition_dd and JPTextEnum.ABOLITION.value in (abolition_dd.get_text(strip=True) or ""))

            pre_li = post.select_one("li.pre")
            prefecture = (pre_li.get_text(strip=True) if pre_li else "").strip()

            stamps.append({
                "post_office_name": post_office_name,
                "fuke_name": fuke_name,
                "abolition": abolition,
                "image_src": img_src,
                "detail_id": detail_id,
                "date": date_val,
                "prefecture": prefecture
            })
        return stamps

    @classmethod
    def _normalize_image_url(cls, img_src: str) -> str:
        src = img_src.strip().replace("/./", "/")
        if not src:
            return ""
        if src.startswith("http"):
            return src
        if src.startswith("//"):
            return f"https:{src}"
        if src.startswith("/"):
            return f"{JPOST_BASE_URL}{src}"
        return f"{FUKE_BASE_URL}/{src}"

    def _fetch_html(self, url: str, timeout: int=DEFAULT_JPOST_REQUEST_TIMEOUT) -> str:
        resp = requests.get(url, headers=FUKE_HEADERS, timeout=timeout)
        resp.raise_for_status()
        resp.encoding = resp.apparent_encoding or "utf-8"
        return resp.text

    def _collect_all_stamps(self, url: str, pref_id: int) -> list[dict]:
        seen_ids: set[str] = set()
        all_stamps: list[dict] = []

        logging.info(f"Requesting for result_pre (pref_id={pref_id}) ...")
        time.sleep(DEFAULT_REQUEST_DELAY)

        html = self._fetch_html(url)
        stamps = self._parse_stamp_posts(html) or []
        for s in stamps:
            if s["detail_id"] not in seen_ids:
                seen_ids.add(s["detail_id"])
                all_stamps.append(s)
        
        logging.info(f"Finish fetching {self._task.owner} initialized page: {len(stamps)}, total stamps {len(all_stamps)}")
        
        page = 1
        while True:
            item_url = f"{FUKE_BASE_URL}/item.php?pref_id={pref_id}&page={page}"
            logging.info(f"Requesting for item.php pref_id={pref_id} page={page} ...")
            time.sleep(DEFAULT_REQUEST_DELAY)
            html = self._fetch_html(item_url)
            stamps = self._parse_stamp_posts(html)
            if not stamps:
                logging.info(f"pref_id={pref_id}, page={page} has no data, finish fetching")
                break
            added = 0
            for s in stamps:
                if s["detail_id"] not in seen_ids:
                    seen_ids.add(s["detail_id"])
                    all_stamps.append(s)
                    added += 1
            logging.info(f"Finish fetching {self._task.owner} page {page}: added {len(stamps)} stamps, total stamps {len(all_stamps)}")
            page += 1

        return all_stamps

    def _download_image(self, url: str, save_path: Path, timeout=DEFAULT_JPOST_REQUEST_TIMEOUT) -> bool:
        try:
            resp = requests.get(url, headers=FUKE_HEADERS, timeout=timeout)
            resp.raise_for_status()
            save_path.parent.mkdir(parents=True, exist_ok=True)
            with open(save_path, "wb") as f:
                f.write(resp.content)
            return True
        except Exception as e:
            logging.error(f"Download image {url} failed: {e}")
            return False

    def _save_image(self, images_dir: Path, stamp: dict) -> str:
        img_src = stamp.get("image_src", "")
        full_img_url = self._normalize_image_url(img_src)
        img_filename = Path(img_src.replace("./", "").replace(".\\", "")).name if img_src else ""
        if not img_filename and full_img_url:
            img_filename = Path(full_img_url).name

        save_path = images_dir / img_filename
        if full_img_url:
            time.sleep(REQUEST_DELAY_BEFORE_DOWNLOAD)
            self._download_image(full_img_url, save_path)
            img_filename = save_path.name

        return img_filename

    def _crawl_prefecture(self) -> int:
        key = self._task.owner
        prefecture = self._load_prefectures()[key]
        url = prefecture.get("url")
        pref_id = prefecture.get("pref_id")
        if not url:
            logging.info(f"Skip prefecture {key}: no url")
            return self.FAILURE
        
        out_dir = TMP_ROOT / key
        images_dir = out_dir / "images"
        if out_dir.exists():
            shutil.rmtree(out_dir)
        images_dir.mkdir(parents=True, exist_ok=True)

        stamps = self._collect_all_stamps(url, pref_id)
        if not stamps:
            logging.info(f"Can not collect stamp from page. prefecture={key}, url={url}")
            return self.FAILURE

        records = []
        for s in stamps:
            img_filename = self._save_image(images_dir, s)
            detail_url = f"{FUKE_BASE_URL}/detail.php?id={s["detail_id"]}"

            record = {
                "post_office_name": s["post_office_name"],
                "fuke_name": s["fuke_name"],
                "abolition": s["abolition"],
                "image": img_filename,
                "detail_url": detail_url,
                "date": s["date"],
                "prefecture": prefecture
            }
            records.append(record)

        data_path = out_dir / "data.json"
        with open(data_path, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        logging.info(f"Data saved for {key}, file path: {data_path}, total records: {len(records)} ")
        return self.SUCCESS

    def fetch(self):
        date = datetime.datetime.now().strftime("%Y-%m-%d")
        ingestor_record = FukeIngestorRecords.get_by_owner_and_date(self._task.owner, date)
        if ingestor_record and ingestor_record.state != FukeIngestorRecords.StateEnum.CREATED.value:
            logging.info(f"Ingestor record exists. task_type={self._task.task_type}, owner={self._task.owner}, date={date}")
            return self.NO_WORK_TO_DO

        if not ingestor_record:
            ingestor_record = FukeIngestorRecords(owner=self._task.owner, date=date)
            success = ingestor_record.save()
            if not success:
                return self.NO_WORK_TO_DO

        result = self._crawl_prefecture()
        if result == self.SUCCESS:
            origin_state = ingestor_record.state
            new_state = FukeIngestorRecords.StateEnum.BASIC.value
            FukeIngestorRecords.update_state(ingestor_record.id, origin_state, new_state)
        return result


class FukeDetailIngestor(BaseIngestor):
    DETAIL_LABEL_MAPPING = {
        "意匠図案説明": "description",
        "図案作成者名": "author",
        "開設場所": "location",
    }

    @classmethod
    def _blank_detail_info(cls) -> dict[str, str]:
        return {field: "" for field in cls.DETAIL_LABEL_MAPPING.values()}

    @classmethod
    def _parse_detail_info(cls, html: str) -> dict[str, str]:
        info = cls._blank_detail_info()
        soup = BeautifulSoup(html, "html.parser")
        for dl in soup.select("div.stampdata dl"):
            dt = dl.find("dt")
            dd = dl.find("dd")

            if not dt or not dd:
                continue
            label = dt.get_text(strip=True)
            key = cls.DETAIL_LABEL_MAPPING.get(label)
            if not key:
                continue
            text = "\n".join(dd.stripped_strings)
            info[key] = text
        return info

    def fetch(self):
        pass