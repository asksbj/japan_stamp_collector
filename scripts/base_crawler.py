"""
日本郵便 風景印 爬虫 - 公共逻辑与常量

供 jpost_fuke_crawler.py（同步）和 jpost_fuke_crawler_async.py（异步）共用。
"""

import json
import re
from pathlib import Path

from bs4 import BeautifulSoup

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DIST_DIR = PROJECT_ROOT / "dist"
PREFECTURE_JSON = DIST_DIR / "prefecture.json"

BASE_URL = "https://www.post.japanpost.jp"
FUKE_BASE = f"{BASE_URL}/kitte_hagaki/stamp/fuke"

DETAIL_LABEL_MAPPING = {
    "意匠図案説明": "description",
    "図案作成者名": "author",
    "開設場所": "location",
}

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.9",
}

REQUEST_DELAY = 1.0


def load_prefectures() -> dict:
    """加载 prefecture.json，返回 {key: {"url": ..., "id": ...}}"""
    with open(PREFECTURE_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)

    result = {}
    for key, val in data.items():
        if isinstance(val, dict):
            if "url" in val:
                result[key] = val
            else:
                raise ValueError(f"prefecture.json 中 {key} 缺少 url 字段")
        else:
            pref_id = _guess_pref_id_from_name(val)
            if pref_id is None:
                raise ValueError(f"无法从 {val} 推断 pref_id，请更新 prefecture.json 格式")
            result[key] = {
                "url": f"{FUKE_BASE}/result_pre.php?pref_id={pref_id}",
                "full_name": val,
            }
    return result


def _guess_pref_id_from_name(name: str) -> int | None:
    """从都道府县名称推断 pref_id（仅用于旧格式兼容）"""
    mapping = {
        "北海道": 1, "青森県": 2, "岩手県": 3, "宮城県": 4, "秋田県": 5,
        "山形県": 6, "福島県": 7, "茨城県": 8, "栃木県": 9, "群馬県": 10,
        "埼玉県": 11, "千葉県": 12, "東京都": 13, "神奈川県": 14, "新潟県": 15,
        "富山県": 16, "石川県": 17, "福井県": 18, "山梨県": 19, "長野県": 20,
        "岐阜県": 21, "静岡県": 22, "愛知県": 23, "三重県": 24, "滋賀県": 25,
        "京都府": 26, "大阪府": 27, "兵庫県": 28, "奈良県": 29, "和歌山県": 30,
        "鳥取県": 31, "島根県": 32, "岡山県": 33, "広島県": 34, "山口県": 35,
        "徳島県": 36, "香川県": 37, "愛媛県": 38, "高知県": 39, "福岡県": 40,
        "佐賀県": 41, "長崎県": 42, "熊本県": 43, "大分県": 44, "宮崎県": 45,
        "鹿児島県": 46, "沖縄県": 47,
    }
    return mapping.get(name)


def parse_stamp_posts(html: str) -> list[dict]:
    """
    从 HTML 中解析 div.post 列表，返回 stamp 信息字典列表。
    每个 dict 包含: name, fuke_name, abolition, image_src, detail_id, date, prefecture
    """
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
        name = (title_dd.get_text(strip=True) if title_dd else "").strip()

        abolition_dd = post.select_one("dd.abolition")
        abolition = bool(abolition_dd and "廃止" in (abolition_dd.get_text(strip=True) or ""))

        pre_li = post.select_one("li.pre")
        prefecture = (pre_li.get_text(strip=True) if pre_li else "").strip()

        stamps.append({
            "name": name,
            "fuke_name": fuke_name,
            "abolition": abolition,
            "image_src": img_src,
            "detail_id": detail_id,
            "date": date_val,
            "prefecture": prefecture,
        })

    return stamps


def blank_detail_info() -> dict[str, str]:
    return {field: "" for field in DETAIL_LABEL_MAPPING.values()}


def parse_detail_info(html: str) -> dict[str, str]:
    """从 detail 页面 HTML 中解析 description/author/location"""
    info = blank_detail_info()
    soup = BeautifulSoup(html, "html.parser")
    for dl in soup.select("div.stampdata dl"):
        dt = dl.find("dt")
        dd = dl.find("dd")
        if not dt or not dd:
            continue
        label = dt.get_text(strip=True)
        key = DETAIL_LABEL_MAPPING.get(label)
        if not key:
            continue
        text = "\n".join(dd.stripped_strings)
        info[key] = text
    return info


def normalize_image_url(img_src: str) -> str:
    """将相对路径转为完整 URL"""
    src = img_src.strip().replace("/./", "/")
    if not src:
        return ""
    if src.startswith("http"):
        return src
    if src.startswith("//"):
        return f"https:{src}"
    if src.startswith("/"):
        return f"{BASE_URL}{src}"
    return f"{FUKE_BASE}/{src}"


def resolve_prefecture_keys(keys: list[str], all_keys: list[str]) -> list[str]:
    """将用户输入的省份名（不区分大小写）解析为 prefecture.json 中的 key"""
    lower_map = {k.lower(): k for k in all_keys}
    resolved = []
    for k in keys:
        low = k.strip().lower()
        if low in lower_map:
            resolved.append(lower_map[low])
        else:
            print(f"  未知省份: {k}（可选: {', '.join(sorted(all_keys))}）")
    return resolved
