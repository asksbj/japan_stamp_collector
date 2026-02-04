#!/usr/bin/env python3
"""
日本郵便 風景印 爬虫脚本（异步版，asyncio + aiohttp）

与 jpost_fuke_crawler.py 功能相同，通过并发请求提升爬取与补全速度。
"""

import argparse
import asyncio
import json
import re
from pathlib import Path

import aiohttp

from base_crawler import (
    FUKE_BASE,
    DETAIL_LABEL_MAPPING,
    DIST_DIR,
    HEADERS,
    PREFECTURE_JSON,
    blank_detail_info,
    load_prefectures,
    normalize_image_url,
    parse_detail_info,
    parse_stamp_posts,
    resolve_prefecture_keys,
)

DEFAULT_CONCURRENT = 20
REQUEST_DELAY = 0.2

DETAIL_CACHE: dict[str, dict[str, str]] = {}


async def fetch_html(session: aiohttp.ClientSession, url: str, semaphore: asyncio.Semaphore) -> str:
    """异步发起 HTTP 请求并返回 HTML 文本"""
    async with semaphore:
        await asyncio.sleep(REQUEST_DELAY)
        async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
            resp.raise_for_status()
            return await resp.text(encoding=resp.charset or "utf-8")


async def fetch_html_simple(session: aiohttp.ClientSession, url: str) -> str:
    """无 semaphore 的简单 fetch，用于顺序调用场景"""
    await asyncio.sleep(REQUEST_DELAY)
    async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        resp.raise_for_status()
        return await resp.text(encoding=resp.charset or "utf-8")


async def get_detail_info(
    session: aiohttp.ClientSession,
    detail_url: str,
    semaphore: asyncio.Semaphore,
) -> dict[str, str]:
    """异步抓取 detail 页面，提取 description/author/location"""
    if not detail_url:
        return blank_detail_info()

    if detail_url in DETAIL_CACHE:
        return DETAIL_CACHE[detail_url]

    html = await fetch_html(session, detail_url, semaphore)
    info = parse_detail_info(html)
    DETAIL_CACHE[detail_url] = info
    return info


async def collect_all_stamps(session: aiohttp.ClientSession, url: str, pref_id: int) -> list[dict]:
    """异步获取完整 stamp 列表"""
    seen_ids: set[str] = set()
    all_stamps: list[dict] = []

    print(f"  请求 result_pre (pref_id={pref_id}) ...")
    html = await fetch_html_simple(session, url)
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
        html = await fetch_html_simple(session, item_url)
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


async def download_image(
    session: aiohttp.ClientSession,
    url: str,
    save_path: Path,
    semaphore: asyncio.Semaphore,
) -> bool:
    """异步下载图片"""
    async with semaphore:
        await asyncio.sleep(0.1)
        try:
            async with session.get(url, headers=HEADERS, timeout=aiohttp.ClientTimeout(total=30)) as resp:
                resp.raise_for_status()
                save_path.parent.mkdir(parents=True, exist_ok=True)
                with open(save_path, "wb") as f:
                    f.write(await resp.read())
            return True
        except Exception as e:
            print(f"    下载失败 {url}: {e}")
            return False


async def crawl_prefecture(
    session: aiohttp.ClientSession,
    key: str,
    prefecture: dict,
    semaphore: asyncio.Semaphore,
) -> None:
    """异步爬取单个省份的数据并保存"""
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

    stamps = await collect_all_stamps(session, url, pref_id)

    async def process_stamp(s: dict) -> dict:
        img_src = s.get("image_src", "")
        full_img_url = normalize_image_url(img_src)
        img_filename = Path(img_src.replace("./", "").replace(".\\", "")).name if img_src else ""
        if not img_filename and full_img_url:
            img_filename = Path(full_img_url).name

        save_path = images_dir / img_filename
        if full_img_url:
            await download_image(session, full_img_url, save_path, semaphore)
            img_filename = save_path.name

        detail_url = f"{FUKE_BASE}/detail.php?id={s['detail_id']}"
        detail_info = await get_detail_info(session, detail_url, semaphore)

        return {
            "jpost_name": s["name"],
            "fuke_name": s["fuke_name"],
            "abolition": s["abolition"],
            "image": img_filename,
            "detail_url": detail_url,
            "date": s["date"],
            "prefecture": s["prefecture"],
            **detail_info,
        }

    records = await asyncio.gather(*[process_stamp(s) for s in stamps])

    data_path = out_dir / "data.json"
    with open(data_path, "w", encoding="utf-8") as f:
        json.dump(list(records), f, ensure_ascii=False, indent=2)

    print(f"  已保存: {data_path} ({len(records)} 条)")


async def enrich_existing_data(
    prefectures_filter: list[str] | None = None,
    concurrent: int = DEFAULT_CONCURRENT,
) -> None:
    """异步遍历 data.json，并行抓取 detail 补全信息"""
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

    semaphore = asyncio.Semaphore(concurrent)
    updated = 0

    async with aiohttp.ClientSession() as session:
        for data_file in data_files:
            with open(data_file, "r", encoding="utf-8") as f:
                try:
                    records = json.load(f)
                except json.JSONDecodeError as exc:
                    print(f"  跳过 {data_file}: 解析失败 ({exc})")
                    continue

            detail_urls = [rec.get("detail_url", "") for rec in records if rec.get("detail_url")]
            if not detail_urls:
                continue

            unique_urls = list(dict.fromkeys(detail_urls))

            async def fetch_one(u: str) -> tuple[str, dict[str, str]]:
                info = await get_detail_info(session, u, semaphore)
                return (u, info)

            results = await asyncio.gather(*[fetch_one(u) for u in unique_urls])
            url_to_info = {u: info for u, info in results}

            dirty = False
            for rec in records:
                url = rec.get("detail_url", "")
                if not url:
                    continue
                info = url_to_info.get(url, blank_detail_info())
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


async def run_crawl(
    pref_filter: list[str] | None,
    prefectures: dict,
    concurrent: int = DEFAULT_CONCURRENT,
) -> None:
    """异步执行爬取"""
    keys = pref_filter if pref_filter else list(prefectures.keys())
    semaphore = asyncio.Semaphore(concurrent)

    async with aiohttp.ClientSession() as session:
        tasks = []
        for key in keys:
            if key not in prefectures:
                print(f"  跳过 {key}: 不在 prefecture.json 中")
                continue
            tasks.append(crawl_prefecture(session, key, prefectures[key], semaphore))

        if tasks:
            await asyncio.gather(*tasks)


def main() -> None:
    parser = argparse.ArgumentParser(description="日本郵便 風景印 数据爬虫（异步，更快）")
    parser.add_argument("--crawl", action="store_true", help="根据 prefecture.json 爬取省份数据")
    parser.add_argument("--enrich", action="store_true", help="抓取 detail 页面补全 data.json")
    parser.add_argument("-p", "--prefecture", type=str, metavar="NAMES", help='指定省份英文名，逗号分隔，如: "Hokkaido,Tokyo,Osaka"')
    parser.add_argument("-j", "--jobs", type=int, default=DEFAULT_CONCURRENT, help=f"并发数 (默认 {DEFAULT_CONCURRENT})")
    args = parser.parse_args()
    concurrent = args.jobs

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
        keys = pref_filter if pref_filter else list(prefectures.keys())
        print(f"将爬取 {len(keys)} 个省份 (并发={concurrent})")
        asyncio.run(run_crawl(pref_filter, prefectures, concurrent))

    if args.enrich:
        print(f"补全详情 (并发={concurrent}) ...")
        asyncio.run(enrich_existing_data(prefectures_filter=pref_filter, concurrent=concurrent))


if __name__ == "__main__":
    main()
