#!/usr/bin/env python3
"""
日本郵便 風景印 爬虫脚本（同步版）

功能：
1. 根据 dist/prefecture.json 爬取各省份风景印列表，保存 data.json 与图片
2. 解析 detail 页面补充 description/author/location
3. 可对 dist 目录下已存在的 data.json 进行详情补全
"""

import argparse
import json
import re
import time
from pathlib import Path

import requests

from base_crawler import (
    FUKE_BASE,
    DETAIL_LABEL_MAPPING,
    DIST_DIR,
    HEADERS,
    PREFECTURE_JSON,
    REQUEST_DELAY,
    blank_detail_info,
    load_prefectures,
    normalize_image_url,
    parse_detail_info,
    parse_stamp_posts,
    resolve_prefecture_keys,
)

DETAIL_CACHE: dict[str, dict[str, str]] = {}


def fetch_html(url: str) -> str:
    """发起 HTTP 请求并返回 HTML 文本"""
    resp = requests.get(url, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def get_detail_info(detail_url: str) -> dict[str, str]:
    """抓取 detail 页面，提取描述/作者/地址等信息"""
    if not detail_url:
        return blank_detail_info()

    if detail_url in DETAIL_CACHE:
        return DETAIL_CACHE[detail_url]

    print(f"    抓取详情: {detail_url}")
    time.sleep(REQUEST_DELAY)
    html = fetch_html(detail_url)
    info = parse_detail_info(html)
    DETAIL_CACHE[detail_url] = info
    return info


def collect_all_stamps(url: str, pref_id: int) -> list[dict]:
    """获取完整 stamp 列表：result_pre + item.php 分页，detail_id 去重"""
    seen_ids: set[str] = set()
    all_stamps: list[dict] = []

    print(f"  请求 result_pre (pref_id={pref_id}) ...")
    time.sleep(REQUEST_DELAY)
    html = fetch_html(url)
    stamps = parse_stamp_posts(html)
    for s in stamps:
        if s["detail_id"] not in seen_ids:
            seen_ids.add(s["detail_id"])
            all_stamps.append(s)
    print(f"    初始页: {len(stamps)} 条，累计: {len(all_stamps)} 条")

    page = 1
    while True:
        item_url = f"{FUKE_BASE}/item.php?pref_id={pref_id}&page={page}"
        print(f"  请求 item.php page={page} ...")
        time.sleep(REQUEST_DELAY)
        html = fetch_html(item_url)
        stamps = parse_stamp_posts(html)
        if not stamps:
            print(f"    page={page} 无数据，结束")
            break
        added = 0
        for s in stamps:
            if s["detail_id"] not in seen_ids:
                seen_ids.add(s["detail_id"])
                all_stamps.append(s)
                added += 1
        print(f"    page={page}: 解析 {len(stamps)} 条，新增 {added} 条，累计: {len(all_stamps)} 条")
        page += 1

    return all_stamps


def download_image(url: str, save_path: Path) -> bool:
    """下载图片到指定路径"""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=30)
        resp.raise_for_status()
        save_path.parent.mkdir(parents=True, exist_ok=True)
        with open(save_path, "wb") as f:
            f.write(resp.content)
        return True
    except Exception as e:
        print(f"    下载失败 {url}: {e}")
        return False


def enrich_existing_data(prefectures_filter: list[str] | None = None) -> None:
    """遍历 dist/*/data.json，根据 detail_url 补充信息"""
    if prefectures_filter:
        data_files = [
            DIST_DIR / key / "data.json"
            for key in prefectures_filter
            if (DIST_DIR / key / "data.json").exists()
        ]
        missing = set(k.lower() for k in prefectures_filter) - set(p.parent.name.lower() for p in data_files)
        if missing:
            print(f"  未找到以下省份目录: {sorted(missing)}")
    else:
        data_files = sorted(DIST_DIR.glob("*/data.json"))

    if not data_files:
        print("未在 dist 目录下找到任何 data.json")
        return

    updated = 0
    for data_file in data_files:
        with open(data_file, "r", encoding="utf-8") as f:
            try:
                records = json.load(f)
            except json.JSONDecodeError as exc:
                print(f"  跳过 {data_file}: 解析失败 ({exc})")
                continue

        dirty = False
        for rec in records:
            detail_url = rec.get("detail_url", "")
            if not detail_url:
                continue
            info = get_detail_info(detail_url)
            for field in DETAIL_LABEL_MAPPING.values():
                value = info.get(field, "")
                if value and rec.get(field) != value:
                    rec[field] = value
                    dirty = True
                elif field not in rec:
                    rec[field] = value
                    dirty = True

        if dirty:
            with open(data_file, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            print(f"  更新 {data_file}")
            updated += 1

    print(f"详情补全完成，更新 {updated} 个文件")


def crawl_prefecture(key: str, prefecture: dict) -> None:
    """爬取单个省份的数据并保存"""
    url = prefecture.get("url")
    pref_id = prefecture.get("id")
    if not url:
        print(f"跳过 {key}: 无 url")
        return
    if pref_id is None:
        m = re.search(r"pref_id=(\d+)", url)
        pref_id = int(m.group(1)) if m else 0

    out_dir = DIST_DIR / key
    images_dir = out_dir / "images"
    images_dir.mkdir(parents=True, exist_ok=True)

    stamps = collect_all_stamps(url, pref_id)
    records = []

    for s in stamps:
        img_src = s.get("image_src", "")
        full_img_url = normalize_image_url(img_src)
        img_filename = Path(img_src.replace("./", "").replace(".\\", "")).name if img_src else ""
        if not img_filename and full_img_url:
            img_filename = Path(full_img_url).name

        save_path = images_dir / img_filename
        if full_img_url:
            time.sleep(0.3)
            download_image(full_img_url, save_path)
            img_filename = save_path.name

        detail_url = f"{FUKE_BASE}/detail.php?id={s['detail_id']}"
        detail_info = get_detail_info(detail_url)

        record = {
            "jpost_name": s["name"],
            "fuke_name": s["fuke_name"],
            "abolition": s["abolition"],
            "image": img_filename,
            "detail_url": detail_url,
            "date": s["date"],
            "prefecture": s["prefecture"],
            **detail_info,
        }
        records.append(record)

    data_path = out_dir / "data.json"
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"  已保存: {data_path} ({len(records)} 条)")


def main() -> None:
    parser = argparse.ArgumentParser(description="日本郵便 風景印 数据爬虫（同步）")
    parser.add_argument("--crawl", action="store_true", help="根据 prefecture.json 爬取省份数据")
    parser.add_argument("--enrich", action="store_true", help="抓取 detail 页面补全 data.json")
    parser.add_argument("-p", "--prefecture", type=str, metavar="NAMES", help='指定省份英文名，逗号分隔，如: "Hokkaido,Tokyo,Osaka"')
    args = parser.parse_args()

    if not args.crawl and not args.enrich:
        args.crawl = True
        args.enrich = True

    pref_filter: list[str] | None = None
    if args.prefecture:
        names = [s.strip() for s in args.prefecture.split(",") if s.strip()]
        with open(PREFECTURE_JSON, "r", encoding="utf-8") as f:
            all_keys = list(json.load(f).keys())
        pref_filter = resolve_prefecture_keys(names, all_keys)
        if not pref_filter:
            print("未解析到任何有效省份，退出")
            return
        print(f"指定省份: {', '.join(pref_filter)}")

    if args.crawl:
        if not PREFECTURE_JSON.exists():
            print(f"错误: 未找到 {PREFECTURE_JSON}")
            return
        prefectures = load_prefectures()
        keys_to_crawl = pref_filter if pref_filter else list(prefectures.keys())
        print(f"将爬取 {len(keys_to_crawl)} 个省份")
        for key in keys_to_crawl:
            if key not in prefectures:
                print(f"  跳过 {key}: 不在 prefecture.json 中")
                continue
            print(f"\n处理: {key}")
            try:
                crawl_prefecture(key, prefectures[key])
            except Exception as e:
                print(f"  失败: {e}")
                raise

    if args.enrich:
        enrich_existing_data(prefectures_filter=pref_filter)


if __name__ == "__main__":
    main()
