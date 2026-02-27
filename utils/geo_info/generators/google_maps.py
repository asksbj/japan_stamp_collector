import re

from utils.geo_info.generators.basic import AbstractGeoInfoGenerator


class GoogleMapsGenerator(AbstractGeoInfoGenerator):
    GEO_VENDOR_NAME = "google maps"
    POSTCODE_RE = re.compile(r"\d{3}-\d{4}")

    
    def __init__(self, **params) -> None:
        self._facility_name = params.get("facility_name")
        self._location = params.get("location", None)
        self._prefecture_ja = params.get("prefecture_ja")
        super().__init__(**params)

    def _extract_postcode(self, text: str) -> str:
        if not text:
            return ""
        m = self.POSTCODE_RE.search(text)
        return m.group(0) if m else ""

    def _get_key(self) -> None:
        if not self._location:
            self._key = self._facility_name
        else:
            location_splits = self._location.split("\n")
            if len(location_splits) != 2:
                self._key = self._facility_name
            else:
                self._key = location_splits[1]

    def _generate_params(self) -> dict[str, str]:
        return {
                "address": self._key,
                "key": self._config.get("api_key"),
                "language": "ja",
            }

    def _parse_results(self) -> None:
        response_status = self._response_data.get("status", "")
        if response_status == "OK":
            self._results = self._response_data.get("results", [])
        
    def _pick_best_result(self) -> None:
        if not self._results:
            return
        if not self._prefecture_ja:
            self._best = self._results[0]
            return

        candidates = [
            r for r in self._results if self._prefecture_ja in (r.get("formatted_address") or "")
        ]
        if not candidates:
            return
        self._best = candidates[0]

    def _parse_geo_info(self) -> dict[str, str]:
        if not self._best:
            return None

        geometry = self._best.get("geometry")
        if not geometry:
            return None
        geo_location = geometry.get("location")
        if not geo_location:
            return None

        address_line = self._best.get("formatted_address")
        return {
            "lat": geo_location.get("lat"),
            "long": geo_location.get("lng"),
            "address_line":address_line,
            "postcode": self._extract_postcode(address_line)
        }
