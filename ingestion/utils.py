import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path


def get_logger(name):
    logging.basicConfig(
        format="%(asctime)s — %(levelname)s — %(message)s",
        level=os.getenv("LOG_LEVEL", "INFO"),
    )
    return logging.getLogger(name)


def load_watermark(state_file):
    """Read the last run date from the state file. Returns None if file doesn't exist."""
    if not os.path.exists(state_file):
        return None

    with open(state_file, "r") as f:
        data = json.load(f)

    last_date = data.get("last_crash_date")
    if last_date:
        return datetime.fromisoformat(last_date).replace(tzinfo=timezone.utc)

    return None


def save_watermark(state_file, last_crash_date):
    """Save the last successfully ingested crash_date to the state file."""
    # Make sure the folder exists
    Path(state_file).parent.mkdir(parents=True, exist_ok=True)

    data = {
        "last_crash_date": last_crash_date.date().isoformat(),
        "updated_at": datetime.now(timezone.utc).isoformat(),
    }

    with open(state_file, "w") as f:
        json.dump(data, f, indent=2)
