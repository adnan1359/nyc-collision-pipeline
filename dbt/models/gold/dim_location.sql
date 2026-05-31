-- Dimension table for crash locations.
-- One row per unique borough + zip_code combination.
-- Latitude and longitude are kept at the fact level since they vary
-- per individual crash even within the same zip code.

with collisions as (

    select * from {{ ref('stg_collisions') }}

),

-- get all distinct borough + zip combinations that appear in the data
unique_locations as (

    select distinct
        borough,
        zip_code
    from collisions

),

final as (

    select
        -- surrogate key - hash of borough and zip_code
        {{ dbt_utils.generate_surrogate_key(['borough', 'zip_code']) }} as location_sk,

        borough,
        zip_code,

        -- label nulls so reports don't show blank rows
        coalesce(borough,  'Unknown') as borough_label,
        coalesce(zip_code, 'Unknown') as zip_code_label

    from unique_locations

)

select * from final
