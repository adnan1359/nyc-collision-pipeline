-- Bronze layer load.
-- Reads the JSONL files that fetch_collisions.py dropped in the Volume and
-- loads them into the Bronze Delta table using COPY INTO.
--
-- COPY INTO tracks which files it has already loaded, so re-running this is safe
-- and won't create duplicates (file-level idempotency).
--
-- Everything lands as STRING - Silver does all the type casting.

-- Create the Bronze table if it doesn't exist yet.
-- Notes:
--   _ingestion_timestamp uses a column DEFAULT so COPY INTO fills it automatically.
--   crash_date_partition is a GENERATED column - Delta computes it from crash_date.
CREATE TABLE IF NOT EXISTS workspace.bronze.collisions_raw (
    collision_id                     STRING,
    crash_date                       STRING,
    crash_time                       STRING,
    borough                          STRING,
    zip_code                         STRING,
    latitude                         STRING,
    longitude                        STRING,
    location                         STRING,
    on_street_name                   STRING,
    cross_street_name                STRING,
    off_street_name                  STRING,
    number_of_persons_injured        STRING,
    number_of_persons_killed         STRING,
    number_of_pedestrians_injured    STRING,
    number_of_pedestrians_killed     STRING,
    number_of_cyclist_injured        STRING,
    number_of_cyclist_killed         STRING,
    number_of_motorist_injured       STRING,
    number_of_motorist_killed        STRING,
    contributing_factor_vehicle_1    STRING,
    contributing_factor_vehicle_2    STRING,
    contributing_factor_vehicle_3    STRING,
    contributing_factor_vehicle_4    STRING,
    contributing_factor_vehicle_5    STRING,
    vehicle_type_code1               STRING,
    vehicle_type_code2               STRING,
    vehicle_type_code_3              STRING,
    vehicle_type_code_4              STRING,
    vehicle_type_code_5              STRING,
    _ingestion_timestamp             TIMESTAMP DEFAULT current_timestamp(),
    crash_date_partition             DATE GENERATED ALWAYS AS (CAST(crash_date AS DATE))
)
USING DELTA
PARTITIONED BY (crash_date_partition)
TBLPROPERTIES (
    'delta.feature.allowColumnDefaults'  = 'supported',
    'delta.autoOptimize.optimizeWrite'   = 'true',
    'delta.autoOptimize.autoCompact'     = 'true'
);

-- Load any new files from the Volume.
--   primitivesAsString = true  -> everything comes in as STRING
--   mergeSchema = true         -> handles records that are missing some fields
--                                 (e.g. a daily file with no vehicle_type_code_5)
COPY INTO workspace.bronze.collisions_raw
FROM '/Volumes/workspace/landing/raw'
FILEFORMAT = JSON
FORMAT_OPTIONS ('primitivesAsString' = 'true')
COPY_OPTIONS ('mergeSchema' = 'true');

-- Compact the small files created by the daily loads. We don't ZORDER here
-- because the table is already partitioned by crash_date_partition, which gives
-- Silver the date-based pruning it needs (you can't ZORDER a partition column).
OPTIMIZE workspace.bronze.collisions_raw;
