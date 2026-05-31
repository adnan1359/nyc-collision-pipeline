-- Monthly rollup of collision metrics.
-- Useful for trend analysis and the Power BI time series charts.

with fact as (

    select * from {{ ref('fact_collisions') }}

),

dates as (

    select * from {{ ref('dim_date') }}

),

monthly as (

    select
        d.year,
        d.month,
        d.month_name,

        -- make a proper date so Power BI can sort it correctly
        date_format(make_date(d.year, d.month, 1), 'yyyy-MM-dd') as month_start_date,

        count(*)                        as total_collisions,
        sum(f.total_injuries)           as total_injuries,
        sum(f.total_fatalities)         as total_fatalities,
        sum(case when f.has_injuries   then 1 else 0 end) as collisions_with_injuries,
        sum(case when f.has_fatalities then 1 else 0 end) as collisions_with_fatalities,

        -- average injuries per collision that month
        round(avg(case when f.total_injuries > 0 then f.total_injuries end), 2)
                                        as avg_injuries_per_crash,

        -- pedestrian-specific
        sum(f.pedestrians_injured)      as pedestrians_injured,
        sum(f.pedestrians_killed)       as pedestrians_killed,

        -- cyclist-specific
        sum(f.cyclists_injured)         as cyclists_injured,
        sum(f.cyclists_killed)          as cyclists_killed

    from fact f
    inner join dates d on d.date_id = f.date_sk
    where d.year is not null
    group by d.year, d.month, d.month_name

)

select * from monthly
order by year, month
