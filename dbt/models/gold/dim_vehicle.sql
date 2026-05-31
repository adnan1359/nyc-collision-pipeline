-- Dimension table for vehicle type combinations involved in a crash.
-- The source has up to 5 vehicle types per collision.
-- One row per unique combination of all 5 vehicle type slots.

with collisions as (

    select * from {{ ref('stg_collisions') }}

),

unique_vehicle_combos as (

    select distinct
        vehicle_type_1,
        vehicle_type_2,
        vehicle_type_3,
        vehicle_type_4,
        vehicle_type_5
    from collisions

),

final as (

    select
        {{ dbt_utils.generate_surrogate_key([
            'vehicle_type_1',
            'vehicle_type_2',
            'vehicle_type_3',
            'vehicle_type_4',
            'vehicle_type_5'
        ]) }}                                   as vehicle_sk,

        vehicle_type_1,
        vehicle_type_2,
        vehicle_type_3,
        vehicle_type_4,
        vehicle_type_5,

        -- the first vehicle type is usually the primary one involved
        coalesce(vehicle_type_1, 'Unknown')     as primary_vehicle_type,

        -- how many vehicle slots are filled in (useful for filtering single-car vs multi-car crashes)
        (
            case when vehicle_type_1 is not null then 1 else 0 end +
            case when vehicle_type_2 is not null then 1 else 0 end +
            case when vehicle_type_3 is not null then 1 else 0 end +
            case when vehicle_type_4 is not null then 1 else 0 end +
            case when vehicle_type_5 is not null then 1 else 0 end
        )                                       as vehicles_involved_count

    from unique_vehicle_combos

)

select * from final
