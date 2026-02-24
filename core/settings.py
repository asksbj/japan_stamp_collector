import os
from pathlib import Path


CORE_ROOT = Path(__file__).resolve().parent
PROJECT_ROOT = CORE_ROOT.parent
TMP_ROOT = PROJECT_ROOT / "tmp"

JPOST_BASE_URL = "https://www.post.japanpost.jp"
FUKE_BASE_URL = f"{JPOST_BASE_URL}/kitte_hagaki/stamp/fuke"
FUKE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.9",
}
JPOST_REQUEST_TIMEOUT = 30
DEFAULT_REQUEST_DELAY = 1.0
REQUEST_DELAY_BEFORE_DOWNLOAD = 0.5

GEO_INFO_REQUEST_TIMEOUT = 30
GEO_INFO_VENDORS = [
    {
        "name": "nominatim",
        "url": "https://nominatim.openstreetmap.org/search",
        "user_agent": "JapanStampCollector/1.0 (https://github.com/japan-stamp-collector; geocoding for post office locations)",
        "request_timeout": 30,
        "max_retries": 2,
        "rate_limit": 1
    },
    {
        "name": "google maps",
        "api_key": os.getenv('GOOGLE_MAPS_API_KEY', 'AIzaSyC3jJxPXJJyUSGhyt7K2aiblZWMxOnjGUg'),
        "url": "https://maps.googleapis.com/maps/api/geocode/json?",
        "user_agent": "JapanStampCollector/1.0; geocoding for post office locations)",
        "request_timeout": 30,
        "max_retries": 2,
        "rate_limit": 1
    },
]
