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
DEFAULT_REQUEST_DELAY = 1.0