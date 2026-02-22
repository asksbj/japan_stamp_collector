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

logging.basicConfig(level=logging.DEBUG)


_last_request_time: float = 0


class PostOfficeLocationIngestor(BaseIngestor):
    GEO_INFO_CACHE: dict[str, dict[str, str]] = {}
    POSTCODE_RE = re.compile(r"\d{3}-\d{4}")

    @classmethod
    async def _fetch_geo_info(
        cls, 
        session: aiohttp.ClientSession, 
        jpost_name: str, 
        prefecture_ja: str,
        use_cache: bool,
        location: str | None = None,
        proxy: str | None = None
    ) -> dict | None:
        cache_key = (jpost_name.strip(), prefecture_ja or "")
        if use_cache and cache_key in cls.GEO_INFO_CACHE:
            return cls.GEO_INFO_CACHE[cache_key]
        
        address = None
        generator_params = {
            "key": jpost_name,
            "jpost_name": jpost_name,
            "prefecture_ja": prefecture_ja,
            "location": location
        }
        generator = None
        for vendor in GEO_INFO_VENDORS:
            vendor_name = vendor.get("name")
            generator = GeoInfoFactory.get_geo_info_generator(vendor_name, **generator_params)

            try:
                address = await generator.generate_geo_info(session, proxy)
            except (asyncio.TimeoutError, aiohttp.ClientError) as e:
                logging.error(f"{vendor_name} request failed for {jpost_name}: {e}")
            
            if address:
                break
        
        if not address:
            logging.debug(f"Can not get geo info for {prefecture_ja}, {jpost_name}")
            return None

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
        no_result_count = 0

        async with aiohttp.ClientSession() as session:
            dirty = False
            for r in records:
                jpost_name = (r.get("post_office_name") or "").strip()
                if not jpost_name:
                    continue
                
                prefecture_ja = r.get("prefecture") or ""
                location = r.get("location") or ""

                address = await self._fetch_geo_info(
                    session,
                    jpost_name,
                    prefecture_ja,
                    use_cache=True,
                    location=location,
                    proxy=proxy
                )
                if address is None:
                    no_result_count += 1
                    logging.debug(f"No location result for {jpost_name}")
                    continue
                else:
                    r["address"] = address
                    updated_count += 1
                    dirty = True

            if dirty:
                with open(data_file, "w", encoding="utf-8") as f:
                    json.dump(records, f, ensure_ascii=False, indent=2)
        logging.info(f"Finished updating location info for {key}: {updated_count} updated, {no_result_count} no result")
        if updated_count:
            return self.SUCCESS
        else:
            return self.NO_WORK_TO_DO
        
    def fetch(self):
        date = datetime.datetime.now().strftime("%Y-%m-%d")
        ingestor_record = FukeIngestorRecords.get_by_owner_and_date(self._task.owner, date)
        if not ingestor_record or ingestor_record.state in [FukeIngestorRecords.StateEnum.CREATED.value, FukeIngestorRecords.StateEnum.BASIC.value]:
            logging.info(f"Fuke ingestor record not ready for location info fetching, task_type={self._task.task_type}, owner={self._task.owner}, date={date}")
            return self.NOT_READY_FOR_WORK
        elif ingestor_record.state != FukeIngestorRecords.StateEnum.DETAILED.value:
            return self.FAILURE
        
        result = asyncio.run(self._get_location_info())
        if result == self.SUCCESS:
            origin_state = ingestor_record.state
            new_state = FukeIngestorRecords.StateEnum.LOCATRED.value
            FukeIngestorRecords.update_state(ingestor_record.id, origin_state, new_state)
        return result