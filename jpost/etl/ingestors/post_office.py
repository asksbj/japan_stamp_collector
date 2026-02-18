import logging
import asyncio
import aiohttp
import datetime
import json
import re
import time

from core.settings import TMP_ROOT, GEO_INFO_VENDORS, GEO_INFO_REQUEST_TIMEOUT
from core.network import get_proxy_from_env
from jpost.etl.ingestors.base import BaseIngestor
from jpost.models.ingestor import FukeIngestorRecords
from jpost.utils.geo_info.factory import GeoInfoFactory

logging.basicConfig(level=logging.INFO)


_last_request_time: float = 0


class PostOfficeLocationIngestor(BaseIngestor):
    GEO_INFO_CACHE: dict[str, dict[str, str]] = {}
    POSTCODE_RE = re.compile(r"\d{3}-\d{4}")

    # @classmethod
    # def _extract_postcode(cls, text: str) -> str:
    #     if not text:
    #         return ""
    #     m = cls.POSTCODE_RE.search(text)
    #     return m.group(0) if m else ""

    # @classmethod
    # def _pick_best_result(cls, results: list[dict], prefecture_ja: str | None) -> dict | None:
    #     if not results:
    #         return None
    #     if not prefecture_ja:
    #         return results[0]

    #     candidates = [
    #         r for r in results if prefecture_ja in (r.get("display_name") or "")
    #     ]
    #     if not candidates:
    #         candidates = results
    #     return candidates[0]

    # @classmethod
    # def _build_address_from_result(cls, result: dict) -> dict:
    #     address_line = result.get("display_name") or ""
    #     return {
    #         "lat": result.get("lat"),
    #         "long": result.get("lon"),
    #         "address_line": address_line,
    #         "postcode": cls._extract_postcode(address_line)
    #     }

    @classmethod
    async def _rate_limited_request(
        cls, 
        session: aiohttp.ClientSession, 
        url: str, 
        params: dict, 
        user_agent: str,
        timeout: float | GEO_INFO_REQUEST_TIMEOUT,
        rate_limit: int | 0,
        max_retries: int | 0,
        proxy: str | None = None,
    ) -> list[dict]:
        global _last_request_time
        timeout = aiohttp.ClientTimeout(total=timeout)
        headers = {
            "User-Agent": user_agent,
            "Accept-Language": "ja",
        }

        last_error = None
        for attemp in range(max_retries+1):
            await asyncio.sleep(max(0, rate_limit - (time.monotonic() - _last_request_time)))
            _last_request_time = time.monotonic()
            try:
                async with session.get(url, params=params, headers=headers, timeout=timeout, proxy=proxy) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                return data if isinstance(data, list) else []
            except (asyncio.TimeoutError, aiohttp.ClientError, aiohttp.ServerDisconnectedError) as e:
                last_error = e
                if attemp < max_retries:
                    await asyncio.sleep(2.0 * (attemp + 1))
                else:
                    raise
        raise last_error

    @classmethod
    async def _fetch_geo_info(
        cls, 
        session: aiohttp.ClientSession, 
        jpost_name: str, 
        prefecture_ja: str,
        use_cache: bool,
        proxy: str | None = None
    ) -> dict | None:
        cache_key = (jpost_name.strip(), prefecture_ja or "")
        if use_cache and cache_key in cls.GEO_INFO_CACHE:
            return cls.GEO_INFO_CACHE[cache_key]
        
        generator_params = {
            "jpost_name": jpost_name,
            "prefecture_ja": prefecture_ja
        }
        results = []
        generator = None
        for vendor in GEO_INFO_VENDORS:
            vendor_name = vendor.get("name")
            generator = GeoInfoFactory.get_geo_info_generator(vendor_name, **generator_params)
            params = generator.generate_params()

            try:
                results = await cls._rate_limited_request(
                    session, 
                    vendor.get("url"), 
                    params,
                    user_agent=vendor.get("user_agent"),
                    timeout=vendor.get("request_timeout"),
                    max_retries=vendor.get("max_retries"),
                    rate_limit=vendor.get("rate_limit"),
                    proxy=proxy)
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                logging.error(f"{vendor_name} request failed for {jpost_name}: {e}")
            
            if results:
                break
        
        if not results:
            logging.debug(f"Can not get geo info for {prefecture_ja}, {jpost_name}")
            return None

        # best = cls._pick_best_result(results, prefecture_ja)
        # best = generator.get_best_result(results)
        # if not best:
        #     return None

        # cls.GEO_INFO_CACHE[cache_key] = best
        # address = cls._build_address_from_result(best)
        address = generator.parse_result()
        if address:
            cls.GEO_INFO_CACHE[cache_key] = address
        return address

    async def _get_location_info(self):
        key = self._task.owner

        data_file = TMP_ROOT / key / "data.json"
        if not data_file.exists():
            logging.error(f"Can not find data.json file for {key}")
            return self.FAILURE

        with open(data_file, "r", encoding="utf-8") as f:
            try:
                records = json.load(f)
            except json.JSONDecodeError as e:
                logging.error(f"Data analysis for {key} failed: {e}")
                return self.FAILURE
        
        proxy = get_proxy_from_env()
        if proxy:
            logging.info(f"Use proxy: {proxy}")

        updated_count = 0
        skipped_cached = 0
        no_result_count = 0

        async with aiohttp.ClientSession() as session:
            dirty = False
            for r in records:
                jpost_name = (r.get("post_office_name") or "").strip()
                if not jpost_name:
                    continue
                if r.get("address") and r.get("address").get("lat") is not None:
                    skipped_cached += 1
                    continue
                
                prefecture_ja = r.get("prefecture") or ""

                address = await self._fetch_geo_info(
                    session,
                    jpost_name,
                    prefecture_ja,
                    use_cache=True,
                    proxy=proxy
                )
                if address is None:
                    no_result_count += 1
                    logging.debug(f"No location result for {jpost_name}")
                    continue

                r["address"] = address
                updated_count += 1
                dirty = True

            if dirty:
                with open(data_file, "w", encoding="utf-8") as f:
                    json.dump(records, f, ensure_ascii=False, indent=2)
        logging.info(f"Finished updating location info for {key}: {updated_count} updated, {skipped_cached} skipped, {no_result_count} no result")
        if updated_count:
            return self.SUCCESS
        else:
            return self.NO_WORK_TO_DO
        
    def fetch(self):
        date = datetime.datetime.now().strftime("%Y-%m-%d")
        ingestor_record = FukeIngestorRecords.get_by_owner_and_date(self._task.owner, date)
        if not ingestor_record or ingestor_record.state != FukeIngestorRecords.StateEnum.DETAILED.value:
            logging.info(f"Fuke ingestor record not ready for location info fetching, task_type={self._task.task_type}, owner={self._task.owner}, date={date}")
        
        result = asyncio.run(self._get_location_info())
        if result == self.SUCCESS:
            origin_state = ingestor_record.state
            new_state = FukeIngestorRecords.StateEnum.LOCATRED.value
            FukeIngestorRecords.update_state(ingestor_record.id, origin_state, new_state)
        return result