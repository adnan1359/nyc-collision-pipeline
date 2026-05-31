-- Fact table for collisions. Grain: one row per collision.
-- Joins to dim_date, dim_location, and dim_vehicle to get surrogate keys.
-- All the actual measures (injury counts, fatalities) live here.

with collisions as (

    select * from {{ ref('stg_collisions') }}

),

dim_date as (

    select * from {{ ref('dim_date') }}

),

dim_location as (

    select * from {{ ref('dim_location') }}

),

dim_vehicle as (

    select * from {{ ref('dim_vehicle') }}

),

-- join everything together to build the fact table
joined as (

    select
        -- surrogate key for this fact row
        {{ dbt_utils.generate_surrogate_key(['c.collision_id']) }} as collision_sk,

        -- natural key - keep it so downstream users can trace back to the source
        c.collision_id,

        -- foreign keys to dimensions
        d.date_id                                                       as date_sk,

        {{ dbt_utils.generate_surrogate_key(['c.borough', 'c.zip_code']) }}
                                                                        as location_sk,

        {{ dbt_utils.generate_surrogate_key([
            'c.vehicle_type_1', 'c.vehicle_type_2', 'c.vehicle_type_3',
            'c.vehicle_type_4', 'c.vehicle_type_5'
        ]) }}                                                           as vehicle_sk,

        -- time columns
        c.crash_date,
        c.crash_time,
        c.crash_datetime,

        -- location columns (kept here for map-based visuals in Power BI)
        c.latitude,
        c.longitude,
        c.on_street_name,
        c.cross_street_name,

        -- injury measures
        c.persons_injured,
        c.persons_killed,
        c.pedestrians_injured,
        c.pedestrians_killed,
        c.cyclists_injured,
        c.cyclists_killed,
        c.motorists_injured,
        c.motorists_killed,
        c.total_injuries,
        c.total_fatalities,
        c.has_injuries,
        c.has_fatalities,

        -- contributing factors
        c.contributing_factor_vehicle_1,
        c.contributing_factor_vehicle_2,
        c.contributing_factor_vehicle_3,
        c.contributing_factor_vehicle_4,
        c.contributing_factor_vehicle_5

    from collisions c

    -- left join so collisions with a bad or missing date still make it into the fact table
    left join dim_date d
        on d.full_date = c.crash_date

)

select * from joined
