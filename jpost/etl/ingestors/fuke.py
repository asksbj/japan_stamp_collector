import re
from bs4 import BeautifulSoup

from core.settings import JPOST_BASE_URL, FUKE_BASE_URL
from jpost.enums.text import JPTextEnum
from jpost.etl.ingestors.base import BaseIngestor
from jpost.models.jpost import Prefecture


class FukeIngestorMixin(object):

    @classmethod
    def _load_prefectures(cls):
        prefecture_dict = {}

        prefectures = Prefecture.get_all()
        for row in prefectures:
            prefecture = Prefecture.from_db(row)
            prefecture_dict.update(prefecture.to_en_dict())
        
        return prefecture_dict
        

class FukeBasicIngestor(FukeIngestorMixin, BaseIngestor):

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
            abolition = bool(abolition_dd and JPTextEnum.ABOLITION in (abolition_dd.get_text(strip=True) or ""))

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

    @classmethod
    def normalize_image_url(cls, img_src: str) -> str:
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

    def fetch(self):
        pass


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