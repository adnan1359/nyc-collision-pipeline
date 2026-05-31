-- Borough-level rollup of collision metrics.
-- One row per borough, covering all time in the dataset.
-- Used for bar charts and borough comparison in Power BI.

with fact as (

    select * from {{ ref('fact_collisions') }}

),

locations as (

    select * from {{ ref('dim_location') }}

),

by_borough as (

    select
        coalesce(l.borough, 'Unknown')  as borough,

        count(*) as total_collisions,
        sum(f.total_injuries) as total_injuries,
        sum(f.total_fatalities) as total_fatalities,

        sum(f.persons_injured) as persons_injured,
        sum(f.persons_killed) as persons_killed,
        sum(f.pedestrians_injured) as pedestrians_injured,
        sum(f.pedestrians_killed) as pedestrians_killed,
        sum(f.cyclists_injured) as cyclists_injured,
        sum(f.cyclists_killed) as cyclists_killed,
        sum(f.motorists_injured) as motorists_injured,
        sum(f.motorists_killed) as motorists_killed,

        -- injury rate = injuries per 100 collisions
        round(sum(f.total_injuries) * 100.0 / count(*), 2)    as injury_rate_per_100,
        round(sum(f.total_fatalities) * 100.0 / count(*), 2)  as fatality_rate_per_100

    from fact f
    left join locations l on l.location_sk = f.location_sk
    group by coalesce(l.borough, 'Unknown')

)

select * from by_borough
order by total_collisions desc
