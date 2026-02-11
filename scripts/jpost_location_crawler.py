#!/usr/bin/env python3
"""
日本郵便 風景印 - Nominatim 位置信息补全脚本

根据 docs/nominatim_usage_policy.md 要求调用 Nominatim API：
- 最多 1 请求/秒
- 自定义 User-Agent 标识应用
- 单线程批量请求，结果本地缓存

使用每条记录的 jpost_name 查询日本境内地址，将结果写入 address 与 en_name 字段。
"""

import argparse
import asyncio
import json
import os
import re
import time
from pathlib import Path

import aiohttp

# Reuse dist paths from base_crawler
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = SCRIPT_DIR.parent
DIST_DIR = PROJECT_ROOT / "dist"
PREFECTURE_JSON = DIST_DIR / "prefecture.json"

NOMINATIM_SEARCH_URL = "https://nominatim.openstreetmap.org/search"
# Per usage policy: custom User-Agent, max 1 req/s
USER_AGENT = "JapanStampCollector/1.0 (https://github.com/japan-stamp-collector; geocoding for post office locations)"
RATE_LIMIT_SECONDS = 1.0
REQUEST_TIMEOUT_SECONDS = 30
MAX_RETRIES = 2

# Cache: (jpost_name, prefecture) -> { lat, lon, display_name, name }
_nominatim_cache: dict[tuple[str, str], dict] = {}
_last_request_time: float = 0


def _get_proxy_from_env() -> str | None:
    """从环境变量读取代理，支持 http_proxy / https_proxy（大小写均可）。"""
    return (
        os.environ.get("https_proxy")
        or os.environ.get("HTTPS_PROXY")
        or os.environ.get("http_proxy")
        or os.environ.get("HTTP_PROXY")
    )


def _pick_best_result(
    results: list[dict],
    prefecture_ja: str | None,
) -> dict | None:
    """
    Select best result using record prefecture and location.
    Nominatim returns display_name like "Yakeyama Post Office, Towada, Aomori Prefecture, 034-0301, Japan".
    """
    if not results:
        return None
    if not prefecture_ja:
        return results[0]

    # Prefer results whose display_name contains the English prefecture (e.g. "Okayama Prefecture" or "Okayama")
    candidates = [
        r
        for r in results
        if prefecture_ja in (r.get("display_name") or "")
    ]
    if not candidates:
        candidates = results

    # If we have record location text, optionally prefer result with higher overlap (simplified: same prefecture is enough)
    return candidates[0]


# Japanese postcode: 3 digits + '-' + 4 digits
POSTCODE_RE = re.compile(r"\d{3}-\d{4}")


def _extract_postcode(text: str) -> str:
    """Extract postcode (3digits-4digits) from text; return empty string if not found."""
    if not text:
        return ""
    m = POSTCODE_RE.search(text)
    return m.group(0) if m else ""


def _build_address_from_result(
    result: dict,
) -> dict:
    """Build address object: lat, long, address_line, postcode."""
    address_line = result.get("display_name") or ""
    return {
        "lat": result.get("lat"),
        "long": result.get("lon"),
        "address_line": address_line,
        "postcode": _extract_postcode(address_line),
    }


async def _rate_limited_request(
    session: aiohttp.ClientSession,
    url: str,
    params: dict,
    proxy: str | None = None,
) -> list[dict]:
    """Perform one Nominatim request respecting 1 request per second. Retries on timeout/connection errors."""
    global _last_request_time
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT_SECONDS)
    headers = {
        "User-Agent": USER_AGENT,
        "Accept-Language": "ja",
    }
    last_err = None
    for attempt in range(MAX_RETRIES + 1):
        await asyncio.sleep(max(0, RATE_LIMIT_SECONDS - (time.monotonic() - _last_request_time)))
        _last_request_time = time.monotonic()
        try:
            async with session.get(url, params=params, headers=headers, timeout=timeout, proxy=proxy) as resp:
                resp.raise_for_status()
                data = await resp.json()
            return data if isinstance(data, list) else []
        except (asyncio.TimeoutError, aiohttp.ClientError, aiohttp.ServerDisconnectedError) as e:
            last_err = e
            if attempt < MAX_RETRIES:
                await asyncio.sleep(2.0 * (attempt + 1))
            else:
                raise
    raise last_err


async def fetch_nominatim(
    session: aiohttp.ClientSession,
    jpost_name: str,
    prefecture_ja: str,
    use_cache: bool,
    proxy: str | None = None,
) -> dict | None:
    """
    Query Nominatim for jpost_name in Japan. Returns address_dict or None.
    """
    cache_key = (jpost_name.strip(), prefecture_ja or "")
    if use_cache and cache_key in _nominatim_cache:
        cached = _nominatim_cache[cache_key]
        return _build_address_from_result(cached)

    params = {
        "q": jpost_name,
        "format": "json",
        "countrycodes": "jp",
    }
    try:
        results = await _rate_limited_request(session, NOMINATIM_SEARCH_URL, params, proxy=proxy)
    except (asyncio.TimeoutError, aiohttp.ClientError) as e:
        print(f"    请求失败 [{jpost_name}]: {e}")
        return None

    best = _pick_best_result(results, prefecture_ja)
    if not best:
        return None

    _nominatim_cache[cache_key] = best
    address = _build_address_from_result(best)
    return address


def _records_to_process(
    prefecture_filter: list[str] | None,
    jpost_name_filter: str | None,
) -> list[tuple[Path, list[dict]]]:
    """
    Collect (data_file, records) to process.
    - prefecture_filter: only dist/{key}/data.json for keys in list.
    - jpost_name_filter: only records where jpost_name equals (after strip).
    """
    if prefecture_filter:
        data_files = [
            DIST_DIR / key / "data.json"
            for key in prefecture_filter
            if (DIST_DIR / key / "data.json").exists()
        ]
    else:
        data_files = sorted(DIST_DIR.glob("*/data.json"))

    out = []
    name_filter = (jpost_name_filter or "").strip().lower()
    for path in data_files:
        try:
            with open(path, "r", encoding="utf-8") as f:
                records = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            print(f"  跳过 {path}: {e}")
            continue
        if not isinstance(records, list):
            continue
        if name_filter:
            records = [r for r in records if (r.get("jpost_name") or "").strip().lower() == name_filter]
        if records:
            out.append((path, records))
    return out


def _apply_address_to_record(rec: dict, address: dict) -> None:
    """Set address, en_name; migrate location into address and remove top-level location."""
    rec["address"] = address


def _deduplicate_records(records: list[dict]) -> tuple[list[dict], int]:
    """
    Remove duplicate records (keep first) using a stable key:
    - Prefer detail_url if present
    - Otherwise (jpost_name, image, date)
    """
    seen = set()
    deduped = []
    removed = 0
    for rec in records:
        key = rec.get("detail_url") or (
            (rec.get("jpost_name") or "").strip(),
            rec.get("image"),
            rec.get("date"),
        )
        if key in seen:
            removed += 1
            continue
        seen.add(key)
        deduped.append(rec)
    return deduped, removed


async def run_update(
    prefecture_filter: list[str] | None,
    jpost_name_filter: str | None,
    force: bool,
) -> None:
    tasks = _records_to_process(prefecture_filter, jpost_name_filter)
    if not tasks:
        print("没有找到需要处理的 data.json 或记录")
        return

    proxy = _get_proxy_from_env()
    if proxy:
        print(f"使用代理: {proxy}")

    total_records = sum(len(rs) for _, rs in tasks)
    updated_count = 0
    skipped_cached = 0
    no_result_count = 0

    async with aiohttp.ClientSession() as session:
        for data_file, records in tasks:
            dirty = False
            for rec in records:
                jpost_name = (rec.get("jpost_name") or "").strip()
                if not jpost_name:
                    continue
                # Skip if already has address and not force
                if not force and rec.get("address") and rec.get("address").get("lat") is not None:
                    skipped_cached += 1
                    continue

                prefecture_ja = rec.get("prefecture") or ""

                address = await fetch_nominatim(
                    session,
                    jpost_name,
                    prefecture_ja,
                    use_cache=True,
                    proxy=proxy,
                )
                if address is None:
                    no_result_count += 1
                    print(f"    无结果 [{jpost_name}]")
                    continue
                _apply_address_to_record(rec, address)
                updated_count += 1
                dirty = True
                print(f"    已更新 [{jpost_name}]")

            deduped, removed = _deduplicate_records(records)
            if dirty or removed > 0:
                with open(data_file, "w", encoding="utf-8") as f:
                    json.dump(deduped, f, ensure_ascii=False, indent=2)
                msg = "已更新" if dirty else "已去重"
                if removed > 0:
                    msg += f" (去重 {removed} 条)"
                print(f"  {msg}: {data_file}")

    print(f"完成: 更新 {updated_count} 条, 跳过(已有) {skipped_cached} 条, 无结果 {no_result_count} 条")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="使用 Nominatim 为 data.json 记录补全 address（遵守 1 请求/秒）",
    )
    parser.add_argument(
        "-p",
        "--prefecture",
        type=str,
        metavar="NAMES",
        help='仅处理指定省份，逗号分隔，如: "Hokkaido,Okayama"',
    )
    parser.add_argument(
        "-n",
        "--name",
        type=str,
        metavar="JPOST_NAME",
        help="仅处理 jpost_name 等于该值的记录（可与 -p 组合）",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="对已有 address 的记录也重新请求并覆盖",
    )
    args = parser.parse_args()

    pref_filter: list[str] | None = None
    if args.prefecture:
        pref_filter = [s.strip() for s in args.prefecture.split(",") if s.strip()]
        if pref_filter:
            print(f"指定省份: {', '.join(pref_filter)}")
    if args.name:
        print(f"指定 jpost_name: {args.name}")

    asyncio.run(run_update(prefecture_filter=pref_filter, jpost_name_filter=args.name, force=args.force))


if __name__ == "__main__":
    main()
