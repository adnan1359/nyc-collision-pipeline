-- Staging model for collisions.
-- Reads from the Silver table and selects/renames the columns we need for the Gold layer.
-- No business logic here - Silver already handled cleaning, typing, and deduplication.

with source as (

    select * from {{ source('silver', 'collisions_clean') }}

),

renamed as (

    select
        -- keys
        collision_id,

        -- time
        crash_date,
        crash_time,
        crash_datetime,

        -- location
        borough,
        zip_code,
        latitude,
        longitude,
        on_street_name,
        cross_street_name,
        off_street_name,

        -- injury counts
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
        has_injuries,
        has_fatalities,

        -- contributing factors
        contributing_factor_vehicle_1,
        contributing_factor_vehicle_2,
        contributing_factor_vehicle_3,
        contributing_factor_vehicle_4,
        contributing_factor_vehicle_5,

        -- vehicle types
        -- note: the API named them inconsistently (code1, code2, code_3...)
        -- we normalize the names here so everything downstream is consistent
        vehicle_type_code1  as vehicle_type_1,
        vehicle_type_code2  as vehicle_type_2,
        vehicle_type_code_3 as vehicle_type_3,
        vehicle_type_code_4 as vehicle_type_4,
        vehicle_type_code_5 as vehicle_type_5

    from source

)

select * from renamed
