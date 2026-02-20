import logging
import asyncio
import aiohttp

from abc import ABC, abstractmethod


class AbstractGeoInfoGenerator(ABC):

    def __init__(self, **kwargs) -> None:
        self._config = kwargs.get("config")
        self._key = kwargs.get("key")
       
    @abstractmethod
    def _generate_params(self) -> dict[str, str]:
        pass

    @abstractmethod
    def _pick_best_result(self, results: list[dict]) -> dict | None:
        pass

    @abstractmethod
    def _parse_result(self, result) -> dict | None:
        pass

    async def _rate_limited_request(self, session: aiohttp.ClientSession, proxy: str | None = None,) -> list[dict]:
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
                return data if isinstance(data, list) else []
            except (asyncio.TimeoutError, aiohttp.ClientError, aiohttp.ServerDisconnectedError) as e:
                last_error = e
                if attemp < max_retries:
                    await asyncio.sleep(2.0 * (attemp + 1))
                else:
                    raise
        raise last_error

    async def generate_result(self, session: aiohttp.ClientSession, proxy: str | None = None,) -> dict | None:
        results = await self._rate_limited_request(session, proxy)
        if not results:
            logging.debug(f"Can not get geo info for {self._key}")
            return None
        # print(results)
        best = self._pick_best_result(results)
        address = self._parse_result(best)
        return address
        