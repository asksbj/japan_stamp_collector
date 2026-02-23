import logging
import asyncio
import aiohttp
from abc import ABC, abstractmethod

from core.settings import GEO_INFO_VENDORS

logging.basicConfig(level=logging.INFO)


class AbstractGeoInfoGenerator(ABC):
    GEO_VENDOR_NAME = "default"

    def __init__(self, **kwargs) -> None:
        self._config = None
        self._key = None
        self._response_data = None
        self._results = []
        self._best = None

        self._get_config()
        self._get_key()

    def _get_config(self) -> None:
        for vendor in GEO_INFO_VENDORS:
            if vendor.get("name") == self.GEO_VENDOR_NAME:
                self._config = vendor

    @abstractmethod
    def _get_key(self) -> None:
        pass
       
    @abstractmethod
    def _generate_params(self) -> dict[str, str]:
        pass

    @abstractmethod
    def _parse_results(self) -> None:
        pass

    @abstractmethod
    def _pick_best_result(self) -> None:
        pass

    @abstractmethod
    def _parse_geo_info(self) -> None:
        pass

    async def _rate_limited_request(self, session: aiohttp.ClientSession, proxy: str | None = None,) -> None:
        url = self._config.get("url")
        max_retries = self._config.get("max_retries")
        timeout = aiohttp.ClientTimeout(total=self._config.get("request_timeout"))
        rate_limit = self._config.get("rate_limit")
        headers = {
            "User-Agent": self._config.get("user_agent"),
            "Accept-Language": "ja",
        }
        params = self._generate_params()
        last_error = None
        for attemp in range(max_retries+1):
            await asyncio.sleep(rate_limit)
            try:
                async with session.get(url, params=params, headers=headers, timeout=timeout, proxy=proxy) as resp:
                    resp.raise_for_status()
                    data = await resp.json()
                self._response_data = data
                return
            except (asyncio.TimeoutError, aiohttp.ClientError, aiohttp.ServerDisconnectedError) as e:
                # last_error = e
                if attemp < max_retries:
                    await asyncio.sleep(2.0 * (attemp + 1))
                else:
                    raise
        raise last_error

    async def generate_geo_info(self, session: aiohttp.ClientSession, proxy: str | None = None,) -> dict | None:
        await self._rate_limited_request(session, proxy)
        self._parse_results()
        self._pick_best_result()
        if not self._best:
            logging.debug(f"Can not get geo info for {self._key}")
            return None
        geo_info = self._parse_geo_info()
        return geo_info
        