from jpost.utils.geo_info.generators.basic import AbstractGeoInfoGenerator


class GoogleMapsGenerator(AbstractGeoInfoGenerator):
    
    # def __init__(self, **params) -> None:
    #     super().__init__(**params) 
    #     self._prefecture_ja = params.get("prefecture_ja")

    def _generate_params(self) -> dict[str, str]:
        return {
                "address": self._key,
                "key": self._config.get("api_key"),
                "language": "ja",
            }
    
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
    
    def _parse_result(self, result: dict) -> dict[str, str]:
        if not result:
            return None

        address_line = result.get("display_name") or ""
        return {
            "lat": result.get("lat"),
            "long": result.get("lon"),
            "address_line": address_line,
            # "postcode": self._extract_postcode(address_line)
        }
