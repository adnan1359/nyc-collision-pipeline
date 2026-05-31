-- Silver layer transformation.
-- Reads from Bronze and produces a clean, typed, deduplicated Silver table.
--
-- We rebuild the whole table each run with CREATE OR REPLACE TABLE AS SELECT.
-- For this dataset (about a year of NYC collisions) that's only a few hundred
-- thousand rows, so a full rebuild on the serverless warehouse takes seconds and
-- is much simpler than maintaining an incremental watermark.
--
-- Steps (as CTEs):
--   casted   -> cast strings to proper types, build crash_datetime
--   cleaned  -> standardize strings, null out bad values, drop unusable rows
--   deduped  -> keep one row per collision_id (latest ingested)
--   totals   -> add total_injuries / total_fatalities

CREATE OR REPLACE TABLE workspace.silver.collisions_clean
USING DELTA
PARTITIONED BY (crash_date_partition)
-- _ingestion_timestamp is carried over from Bronze, which has a column DEFAULT,
-- so the new table needs this feature flag enabled too
TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
AS

WITH casted AS (
    SELECT
        collision_id,
        CAST(crash_date AS DATE)                          AS crash_date,
        TRIM(crash_time)                                  AS crash_time,

        -- combine date + time into one timestamp (default time to 0:00 if missing)
        -- the source uses single-digit hours like "3:10" so the format is H:mm
        -- try_to_timestamp returns NULL for any value that still wont parse
        -- instead of failing the whole job
        TRY_TO_TIMESTAMP(
            CONCAT(
                CAST(CAST(crash_date AS DATE) AS STRING),
                ' ',
                COALESCE(NULLIF(TRIM(crash_time), ''), '0:00')
            ),
            'yyyy-MM-dd H:mm'
        )                                                 AS crash_datetime,

        UPPER(TRIM(borough))                              AS borough,
        TRIM(zip_code)                                    AS zip_code,
        CAST(latitude  AS DOUBLE)                         AS latitude,
        CAST(longitude AS DOUBLE)                         AS longitude,
        UPPER(TRIM(on_street_name))                       AS on_street_name,
        UPPER(TRIM(cross_street_name))                    AS cross_street_name,
        UPPER(TRIM(off_street_name))                      AS off_street_name,

        CAST(number_of_persons_injured     AS INT)        AS persons_injured,
        CAST(number_of_persons_killed      AS INT)        AS persons_killed,
        CAST(number_of_pedestrians_injured AS INT)        AS pedestrians_injured,
        CAST(number_of_pedestrians_killed  AS INT)        AS pedestrians_killed,
        CAST(number_of_cyclist_injured     AS INT)        AS cyclists_injured,
        CAST(number_of_cyclist_killed      AS INT)        AS cyclists_killed,
        CAST(number_of_motorist_injured    AS INT)        AS motorists_injured,
        CAST(number_of_motorist_killed     AS INT)        AS motorists_killed,

        INITCAP(TRIM(contributing_factor_vehicle_1))      AS contributing_factor_vehicle_1,
        INITCAP(TRIM(contributing_factor_vehicle_2))      AS contributing_factor_vehicle_2,
        INITCAP(TRIM(contributing_factor_vehicle_3))      AS contributing_factor_vehicle_3,
        INITCAP(TRIM(contributing_factor_vehicle_4))      AS contributing_factor_vehicle_4,
        INITCAP(TRIM(contributing_factor_vehicle_5))      AS contributing_factor_vehicle_5,

        UPPER(TRIM(vehicle_type_code1))                   AS vehicle_type_code1,
        UPPER(TRIM(vehicle_type_code2))                   AS vehicle_type_code2,
        UPPER(TRIM(vehicle_type_code_3))                  AS vehicle_type_code_3,
        UPPER(TRIM(vehicle_type_code_4))                  AS vehicle_type_code_4,
        UPPER(TRIM(vehicle_type_code_5))                  AS vehicle_type_code_5,

        _ingestion_timestamp
    FROM workspace.bronze.collisions_raw
),

cleaned AS (
    SELECT
        collision_id,
        crash_date,
        crash_time,
        crash_datetime,

        -- turn empty strings into proper nulls
        NULLIF(borough, '')                               AS borough,
        NULLIF(zip_code, '')                              AS zip_code,

        -- (0,0) coordinates are in the ocean off Africa - clearly bad data
        CASE WHEN latitude  = 0 THEN NULL ELSE latitude  END   AS latitude,
        CASE WHEN longitude = 0 THEN NULL ELSE longitude END   AS longitude,

        NULLIF(on_street_name, '')                        AS on_street_name,
        NULLIF(cross_street_name, '')                     AS cross_street_name,
        NULLIF(off_street_name, '')                       AS off_street_name,

        -- negative counts are bad data -> null
        CASE WHEN persons_injured     < 0 THEN NULL ELSE persons_injured     END AS persons_injured,
        CASE WHEN persons_killed      < 0 THEN NULL ELSE persons_killed      END AS persons_killed,
        CASE WHEN pedestrians_injured < 0 THEN NULL ELSE pedestrians_injured END AS pedestrians_injured,
        CASE WHEN pedestrians_killed  < 0 THEN NULL ELSE pedestrians_killed  END AS pedestrians_killed,
        CASE WHEN cyclists_injured    < 0 THEN NULL ELSE cyclists_injured    END AS cyclists_injured,
        CASE WHEN cyclists_killed     < 0 THEN NULL ELSE cyclists_killed     END AS cyclists_killed,
        CASE WHEN motorists_injured   < 0 THEN NULL ELSE motorists_injured   END AS motorists_injured,
        CASE WHEN motorists_killed    < 0 THEN NULL ELSE motorists_killed    END AS motorists_killed,

        NULLIF(contributing_factor_vehicle_1, '')         AS contributing_factor_vehicle_1,
        NULLIF(contributing_factor_vehicle_2, '')         AS contributing_factor_vehicle_2,
        NULLIF(contributing_factor_vehicle_3, '')         AS contributing_factor_vehicle_3,
        NULLIF(contributing_factor_vehicle_4, '')         AS contributing_factor_vehicle_4,
        NULLIF(contributing_factor_vehicle_5, '')         AS contributing_factor_vehicle_5,

        NULLIF(vehicle_type_code1, '')                    AS vehicle_type_code1,
        NULLIF(vehicle_type_code2, '')                    AS vehicle_type_code2,
        NULLIF(vehicle_type_code_3, '')                   AS vehicle_type_code_3,
        NULLIF(vehicle_type_code_4, '')                   AS vehicle_type_code_4,
        NULLIF(vehicle_type_code_5, '')                   AS vehicle_type_code_5,

        _ingestion_timestamp
    FROM casted
    -- drop rows we can't use: no id (can't dedupe) or no date (can't put in time series)
    WHERE collision_id IS NOT NULL
      AND collision_id <> 'None'
      AND crash_date   IS NOT NULL
),

deduped AS (
    SELECT
        *,
        -- keep the most recently ingested version of each collision
        ROW_NUMBER() OVER (
            PARTITION BY collision_id
            ORDER BY _ingestion_timestamp DESC
        ) AS rn
    FROM cleaned
),

totals AS (
    SELECT
        * EXCEPT (rn),
        (COALESCE(persons_injured, 0)
            + COALESCE(pedestrians_injured, 0)
            + COALESCE(cyclists_injured, 0)
            + COALESCE(motorists_injured, 0))             AS total_injuries,
        (COALESCE(persons_killed, 0)
            + COALESCE(pedestrians_killed, 0)
            + COALESCE(cyclists_killed, 0)
            + COALESCE(motorists_killed, 0))              AS total_fatalities
    FROM deduped
    WHERE rn = 1
)

SELECT
    collision_id,
    crash_date,
    crash_time,
    crash_datetime,
    borough,
    zip_code,
    latitude,
    longitude,
    on_street_name,
    cross_street_name,
    off_street_name,
    persons_injured,
    persons_killed,
    pedestrians_injured,
    pedestrians_killed,
    cyclists_injured,
    cyclists_killed,
    motorists_injured,
    motorists_killed,
    total_injuries,
    total_fatalities,
    (total_injuries   > 0)                                AS has_injuries,
    (total_fatalities > 0)                                AS has_fatalities,
    contributing_factor_vehicle_1,
    contributing_factor_vehicle_2,
    contributing_factor_vehicle_3,
    contributing_factor_vehicle_4,
    contributing_factor_vehicle_5,
    vehicle_type_code1,
    vehicle_type_code2,
    vehicle_type_code_3,
    vehicle_type_code_4,
    vehicle_type_code_5,
    _ingestion_timestamp,
    current_timestamp()                                   AS _silver_timestamp,
    crash_date                                            AS crash_date_partition
FROM totals;
