from utils.geo_info.generators.nominatim import NominatimGeoGenerator
from utils.geo_info.generators.google_maps import GoogleMapsGenerator


class GeoInfoFactory:

    @classmethod
    def get_geo_info_generator(cls, name, **params):
        vendor = None

        if name == "nominatim":
            vendor = NominatimGeoGenerator(**params)
        elif name == "google maps":
            vendor = GoogleMapsGenerator(**params)

        return vendor