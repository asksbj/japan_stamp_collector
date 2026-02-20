import re

from jpost.utils.geo_info.generators.basic import AbstractGeoInfoGenerator


class NominatimGeoGenerator(AbstractGeoInfoGenerator):
    # CITY_RE = re.compile()
    POSTCODE_RE = re.compile(r"\d{3}-\d{4}")

    def __init__(self, **params) -> None:
        super().__init__(**params) 
        self._prefecture_ja = params.get("prefecture_ja")
    
    def _pick_best_result(self, results: list[dict]) -> dict | None:
        if not results:
            return None
        if not self._prefecture_ja:
            return results[0]

        candidates = [
            r for r in results if self._prefecture_ja in (r.get("display_name") or "")
        ]
        if not candidates:
            return None
        return candidates[0]

    def _extract_postcode(self, text: str) -> str:
        if not text:
            return ""
        m = self.POSTCODE_RE.search(text)
        return m.group(0) if m else ""

    # def _extract_city(self, text: str) -> str:
    #     if not text:
    #         return ""
    #     m = self.POSTCODE_RE.search(text)
    #     return m.group(0) if m else ""
    
    def _generate_params(self) -> dict[str, str]:
        return {
                "q": self._key,
                "format": "json",
                "countrycodes": "jp",
            }

    def _parse_result(self, result: dict) -> dict[str, str]:
        if not result:
            return None

        address_line = result.get("display_name") or ""
        return {
            "lat": result.get("lat"),
            "long": result.get("lon"),
            "address_line": address_line,
            "postcode": self._extract_postcode(address_line)
        }

