import re

from jpost.utils.geo_info.generators.basic import AbstractGeoInfoGenerator


class NominatimGeoGenerator(AbstractGeoInfoGenerator):
    GEO_VENDOR_NAME = "nominatim"
    # CITY_RE = re.compile()
    POSTCODE_RE = re.compile(r"\d{3}-\d{4}")

    def __init__(self, **params) -> None:
        self._jpost_name = params.get("jpost_name")
        self._prefecture_ja = params.get("prefecture_ja")
        super().__init__(**params)

    def _get_key(self) -> None:
        self._key = self._jpost_name

    def _generate_params(self) -> dict[str, str]:
        return {
                "q": self._key,
                "format": "json",
                "countrycodes": "jp",
            }

    def _parse_results(self) -> None:
        self._results = self._response_data if isinstance(self._response_data, list) else []
    
    def _pick_best_result(self) -> None:
        if not self._results:
            return
        if not self._prefecture_ja:
            self._best = self._results[0]
            return

        candidates = [
            r for r in self._results if self._prefecture_ja in (r.get("display_name") or "")
        ]
        if not candidates:
            return
        self._best = candidates[0]

    def _extract_postcode(self, text: str) -> str:
        if not text:
            return ""
        m = self.POSTCODE_RE.search(text)
        return m.group(0) if m else ""

    # TODO: Get city info in the future
    def _extract_city(self, text: str) -> str:
        pass

    def _parse_geo_info(self) -> dict[str, str]:
        if not self._best:
            return None

        address_line = self._best.get("display_name") or ""
        return {
            "lat": self._best.get("lat"),
            "long": self._best.get("lon"),
            "address_line": address_line,
            "postcode": self._extract_postcode(address_line)
        }

