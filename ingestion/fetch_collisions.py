"""
Ingestion script for NYC Motor Vehicle Collisions data.

Fetches data from the NYC Open Data API and uploads it as a JSONL file to a
Unity Catalog Volume in Databricks. The Bronze notebook picks it up from there.

No Spark or PySpark needed here - just requests + databricks-sdk.

Usage:
    python fetch_collisions.py               # normal incremental run
    python fetch_collisions.py --full-reload  # re-pull last year
"""

import argparse
import io
import json
import sys
import time
from datetime import datetime, timedelta, timezone

import requests
from databricks.sdk import WorkspaceClient

import config
import utils

logger = utils.get_logger(__name__)


def fetch_page(session, where_clause, offset):
    """Fetch one page of data from the Socrata API with retry logic."""

    params = {
        "$where": where_clause,
        "$limit": config.PAGE_SIZE,
        "$offset": offset,
        "$order": "crash_date ASC, collision_id ASC",
    }

    for attempt in range(1, config.MAX_RETRIES + 1):
        try:
            resp = session.get(config.API_URL, params=params, timeout=config.REQUEST_TIMEOUT)
            resp.raise_for_status()
            return resp.json()

        except requests.RequestException as e:
            if attempt == config.MAX_RETRIES:
                logger.error("Failed after %d attempts: %s", config.MAX_RETRIES, e)
                raise

            wait_time = config.RETRY_WAIT * (2 ** (attempt - 1))
            logger.warning("Attempt %d failed: %s. Retrying in %ds...", attempt, e, wait_time)
            time.sleep(wait_time)


def fetch_all_records(start_date, end_date):
    """Paginate through the API and return all records in the date window."""

    start_str = start_date.strftime("%Y-%m-%dT%H:%M:%S.000")
    end_str   = end_date.strftime("%Y-%m-%dT%H:%M:%S.000")
    where     = f"crash_date > '{start_str}' AND crash_date <= '{end_str}'"

    logger.info("Fetching records from %s to %s", start_date.date(), end_date.date())

    all_records = []
    offset = 0

    with requests.Session() as session:
        while True:
            logger.info("Fetching page at offset %d (total so far: %d)", offset, len(all_records))

            page = fetch_page(session, where, offset)
            all_records.extend(page)

            if len(page) < config.PAGE_SIZE:
                break

            offset += config.PAGE_SIZE

    logger.info("Done fetching. Total records: %d", len(all_records))
    return all_records


def upload_to_volume(records, batch_id):
    """
    Upload the fetched records as a JSONL file to the Unity Catalog Volume.

    The Bronze notebook will run COPY INTO from this Volume path to load
    the data into the Bronze Delta table - no Spark needed on this side.
    """

    # Flatten any nested objects to JSON strings.
    # The API returns the `location` field as a nested dict - if we leave it
    # as-is, COPY INTO will fail trying to insert a JSON object into a STRING column.
    flat_records = []
    for record in records:
        flat = {
            k: json.dumps(v) if isinstance(v, (dict, list)) else v
            for k, v in record.items()
        }
        flat_records.append(flat)

    # Write as newline-delimited JSON (one record per line)
    # COPY INTO in Databricks works well with this format
    jsonl = "\n".join(json.dumps(record) for record in flat_records)
    content = io.BytesIO(jsonl.encode("utf-8"))

    filename    = f"batch_{batch_id}.json"
    volume_path = f"{config.VOLUME_PATH}/{filename}"

    logger.info("Uploading %d records to %s", len(records), volume_path)

    w = WorkspaceClient(
        host  = config.DATABRICKS_HOST,
        token = config.DATABRICKS_TOKEN,
    )

    w.files.upload(volume_path, content, overwrite=True)
    logger.info("Upload complete: %s", volume_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--full-reload",
        action="store_true",
        help="Ignore watermark and re-pull the last year of data",
    )
    args = parser.parse_args()

    run_time = datetime.now(timezone.utc)
    batch_id = run_time.strftime("%Y%m%d_%H%M%S")

    logger.info("Starting ingestion run — batch_id=%s", batch_id)

    # Figure out the date range to fetch
    today     = run_time.replace(hour=0, minute=0, second=0, microsecond=0)
    fetch_end = today - timedelta(seconds=1)  # end of yesterday

    if args.full_reload:
        fetch_start = today - timedelta(days=config.INITIAL_LOAD_DAYS)
        logger.info("Full reload - fetching from %s", fetch_start.date())
    else:
        watermark = utils.load_watermark(config.STATE_FILE)
        if watermark is None:
            fetch_start = today - timedelta(days=config.INITIAL_LOAD_DAYS)
            logger.info("No watermark found, doing initial load from %s", fetch_start.date())
        else:
            fetch_start = watermark
            logger.info("Found watermark: %s", fetch_start.date())

    if fetch_start >= fetch_end:
        logger.info("Already up to date, nothing to fetch.")
        sys.exit(0)

    # Pull data from the API
    try:
        records = fetch_all_records(fetch_start, fetch_end)
    except Exception as e:
        logger.error("Failed to fetch data from API: %s", e)
        sys.exit(1)

    if len(records) == 0:
        logger.info("No new records found, updating watermark anyway.")
        utils.save_watermark(config.STATE_FILE, fetch_end)
        sys.exit(0)

    # Upload to the Unity Catalog Volume
    # The Bronze notebook takes it from here
    try:
        upload_to_volume(records, batch_id)
    except Exception as e:
        logger.error("Failed to upload to Volume: %s", e)
        # Don't update the watermark - retry this window next run
        sys.exit(1)

    # Advance watermark to the max crash_date we actually got back
    max_date_str  = max(r["crash_date"] for r in records if r.get("crash_date"))
    new_watermark = datetime.fromisoformat(max_date_str.rstrip("Z")).replace(tzinfo=timezone.utc)

    utils.save_watermark(config.STATE_FILE, new_watermark)
    logger.info("Watermark updated to %s", new_watermark.date())

    elapsed = (datetime.now(timezone.utc) - run_time).total_seconds()
    logger.info("Done in %.1f seconds", elapsed)


if __name__ == "__main__":
    main()
