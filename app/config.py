from pathlib import Path


BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
BRONZE_DIR = DATA_DIR / "bronze"
SILVER_DIR = DATA_DIR / "silver"
GOLD_DIR = DATA_DIR / "gold"

OPEN_BREWERY_API_URL = "https://api.openbrewerydb.org/v1/breweries"
DEFAULT_PAGE_SIZE = 200
DEFAULT_REQUEST_TIMEOUT = 30
