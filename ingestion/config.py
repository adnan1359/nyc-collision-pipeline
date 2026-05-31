import os

# NYC Open Data API for motor vehicle collisions
API_URL = "https://data.cityofnewyork.us/resource/h9gi-nx95.json"

# Socrata allows max 50000 rows per request
PAGE_SIZE = 50000

REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 5
RETRY_WAIT = 2  # seconds, doubles each retry

# On first run we pull 1 year of data
INITIAL_LOAD_DAYS = 365

# File to store the last run date so we can do incremental loads
STATE_FILE = "ingestion/state/last_run_state.json"

# Databricks connection - set these as environment variables or in .env
DATABRICKS_HOST  = os.getenv("DATABRICKS_HOST")
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN")

# Unity Catalog Volume where we drop the raw JSON files before Bronze picks them up.
# Path format: /Volumes/<catalog>/<schema>/<volume>
# We use the built-in "workspace" catalog because Databricks Free Edition does
# not let you create new top-level catalogs.
VOLUME_PATH = "/Volumes/workspace/landing/raw"

# Bronze table in Unity Catalog
BRONZE_TABLE = "workspace.bronze.collisions_raw"
