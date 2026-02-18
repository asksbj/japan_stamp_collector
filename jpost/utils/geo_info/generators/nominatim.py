import re

from utils.geo_info.generators.basic import AbstractGeoInfoGenerator


class NominatimGeoGenerator(AbstractGeoInfoGenerator):
    POSTCODE_RE = re.compile(r"\d{3}-\d{4}")

    def __init__(self, **params) -> None:
        self._jpost_name = params.get("jpost_name")
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
            candidates = results
        return candidates[0]

    def _extract_postcode(self, text: str) -> str:
        if not text:
            return ""
        m = self.POSTCODE_RE.search(text)
        return m.group(0) if m else ""
    
    def generate_params(self) -> dict[str, str]:
        return {
                "q": self._jpost_name,
                "format": "json",
                "countrycodes": "jp",
            }

    def parse_result(self, results: list[dict]) -> dict[str, str]:
        best = self._pick_best_result(results)
        if not best:
            return None

        address_line = best.get("display_name") or ""
        return {
            "lat": best.get("lat"),
            "long": best.get("lon"),
            "address_line": address_line,
            "postcode": self._extract_postcode(address_line)
        }

