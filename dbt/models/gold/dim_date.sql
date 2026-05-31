-- Dimension table for dates.
-- Generates a row for every date between 2022-01-01 and 2030-12-31 so we never
-- get a missing date key even if the source data has gaps.

with date_spine as (

    -- dbt_utils.date_spine generates one row per day between the two dates
    {{ dbt_utils.date_spine(
        datepart = "day",
        start_date = "cast('2022-01-01' as date)",
        end_date   = "cast('2030-12-31' as date)"
    ) }}

),

final as (

    select
        -- using YYYYMMDD as an integer surrogate key - simple and readable
        cast(date_format(date_day, 'yyyyMMdd') as int)  as date_id,
        date_day                                         as full_date,

        year(date_day)                                   as year,
        quarter(date_day)                                as quarter,
        month(date_day)                                  as month,
        date_format(date_day, 'MMMM')                   as month_name,
        day(date_day)                                    as day,
        weekofyear(date_day)                             as week_of_year,

        -- dayofweek returns 1=Sunday, 7=Saturday in Spark SQL
        dayofweek(date_day)                              as day_of_week,
        date_format(date_day, 'EEEE')                   as day_name,

        case
            when dayofweek(date_day) in (1, 7) then true
            else false
        end                                              as is_weekend

    from date_spine

)

select * from final
