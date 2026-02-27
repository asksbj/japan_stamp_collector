import asyncio
import logging
import sys
from pathlib import Path
from typing import Dict, Tuple

import aiohttp

# Allow running as a plain script (cron friendly):
#   python3 scripts/crons/daily_update_geo_info.py
# by ensuring the project root is on sys.path so imports like `core.*` work.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.network import get_proxy_from_env
from core.settings import GEO_INFO_VENDORS
from models.administration import Facility, Prefecture
from utils.geo_info.factory import GeoInfoFactory


logging.basicConfig(level=logging.INFO)


GEO_INFO_CACHE: Dict[Tuple[str, str], dict] = {}


async def _fetch_geo_info_for_facility(
    session: aiohttp.ClientSession,
    facility: Facility,
    prefecture_name_ja: str,
    proxy: str | None,
    use_cache: bool = True,
) -> dict | None:
    """
    Use configured geo info generators to fetch postcode / lat / long / address for a facility.

    The generators return a dict like:
        {
            "lat": "...",
            "long": "...",
            "address_line": "...",
            "postcode": "123-4567",
        }
    """
    key_name = (facility.name or "").strip()
    cache_key = (key_name, prefecture_name_ja or "")

    if not key_name:
        return None

    if use_cache and cache_key in GEO_INFO_CACHE:
        return GEO_INFO_CACHE[cache_key]

    location = facility.address or ""

    geo_info: dict | None = None
    generator = None

    for vendor in GEO_INFO_VENDORS:
        vendor_name = vendor.get("name")
        generator_params = {
            "facility_name": key_name,
            "prefecture_ja": prefecture_name_ja,
            "location": location or None,
        }
        generator = GeoInfoFactory.get_geo_info_generator(vendor_name, **generator_params)
        if not generator:
            continue

        try:
            logging.info(f"Fetching geo info for facility '{key_name}' ({prefecture_name_ja}) with {vendor_name}")
            geo_info = await generator.generate_geo_info(session, proxy)
        except (asyncio.TimeoutError, aiohttp.ClientError) as e:
            logging.error(f"{vendor_name} request failed for facility '{key_name}': {e}")

        if geo_info:
            break

    if not geo_info:
        logging.debug(f"Can not get geo info for facility '{key_name}' ({prefecture_name_ja})")
        return None

    GEO_INFO_CACHE[cache_key] = geo_info
    return geo_info


async def update_facilities_geo_info() -> None:
    """
    Find all Facility rows that miss any of postcode / latitude / longtitude,
    fetch geo info for them and update the database.
    """
    # Build prefecture id -> full_name (Japanese) map for better geocoding.
    prefectures = Prefecture.get_all()
    pref_by_id: Dict[int, Prefecture] = {p.pref_id: p for p in prefectures if p.pref_id}

    facilities = Facility.get_without_geo_info()

    if not facilities:
        logging.info("No Facility records need geo info update.")
        return

    logging.info(f"Found {len(facilities)} Facility records missing geo info.")

    proxy = get_proxy_from_env()

    updated_count = 0
    no_result_count = 0
    error_count = 0

    async with aiohttp.ClientSession() as session:
        for facility in facilities:
            pref = pref_by_id.get(facility.pref_id)
            prefecture_name_ja = pref.full_name if pref else ""

            geo = await _fetch_geo_info_for_facility(
                session=session,
                facility=facility,
                prefecture_name_ja=prefecture_name_ja,
                proxy=proxy,
                use_cache=True,
            )
            if not geo:
                no_result_count += 1
                continue

            changed = False

            postcode = geo.get("postcode") or ""
            if not facility.postcode and postcode:
                facility.postcode = postcode
                changed = True

            lat = geo.get("lat")
            if not facility.latitude and lat is not None:
                facility.latitude = str(lat)
                changed = True

            lng = geo.get("long")
            if not facility.longtitude and lng is not None:
                facility.longtitude = str(lng)
                changed = True

            addr_line = geo.get("address_line") or ""
            if not facility.address and addr_line:
                facility.address = addr_line
                changed = True

            if not changed:
                continue

            success = facility.save()
            if not success:
                error_count += 1
                logging.error(f"Failed to save Facility(id={facility.id}, name={facility.name})")
            else:
                updated_count += 1

    logging.info(
        "Finished daily Facility geo info update: %d updated, %d no result, %d errors",
        updated_count,
        no_result_count,
        error_count,
    )


def main() -> None:
    """
    Entry point for cron.

    Usage (example):
        python -m scripts.crons.daily_update_geo_info
    or:
        python scripts/crons/daily_update_geo_info.py
    """
    asyncio.run(update_facilities_geo_info())


if __name__ == "__main__":
    main()

