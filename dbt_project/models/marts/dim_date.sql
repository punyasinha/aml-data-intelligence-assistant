{{
    config(
        materialized='table',
        tags=['marts', 'dimension', 'date']
    )
}}

/*
    Dimension: dim_date
    Grain: one row per calendar date

    Standard date spine covering 2017-01-01 to 2026-12-31.
    Conformed dimension used across all fact tables for consistent
    time-based slicing and period comparisons.
*/

with date_spine as (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2017-01-01' as date)",
        end_date="cast('2026-12-31' as date)"
    ) }}

),

final as (

    select
        cast(date_day as date)                                         as date_id,
        date_day                                                       as full_date,

        -- calendar attributes
        date_part('year', date_day)                                    as calendar_year,
        date_part('quarter', date_day)                                 as calendar_quarter,
        date_part('month', date_day)                                   as calendar_month,
        monthname(date_day)                                            as month_name,
        left(monthname(date_day), 3)                                   as month_name_short,
        date_part('week', date_day)                                    as calendar_week,
        date_part('day', date_day)                                     as day_of_month,
        date_part('dayofyear', date_day)                               as day_of_year,
        date_part('dow', date_day)                                     as day_of_week,  -- 0=Sun
        dayname(date_day)                                              as day_name,
        left(dayname(date_day), 3)                                     as day_name_short,

        -- business calendar flags
        case
            when date_part('dow', date_day) in (0, 6) then false
            else true
        end                                                            as is_weekday,
        case
            when date_part('dow', date_day) in (0, 6) then true
            else false
        end                                                            as is_weekend,
        case
            when date_part('day', date_day) = 1 then true
            else false
        end                                                            as is_month_start,
        case
            when date_day = last_day(date_day) then true
            else false
        end                                                            as is_month_end,

        -- fiscal year (Australian — Jul to Jun)
        case
            when date_part('month', date_day) >= 7
            then date_part('year', date_day)
            else date_part('year', date_day) - 1
        end                                                            as fiscal_year,
        case
            when date_part('month', date_day) >= 7
            then date_part('month', date_day) - 6
            else date_part('month', date_day) + 6
        end                                                            as fiscal_month,

        -- relative flags
        case when date_day = current_date() then true else false end   as is_today,
        case when date_day < current_date() then true else false end   as is_past,

        -- formatted strings
        to_varchar(date_day, 'YYYY-MM-DD')                            as date_string,
        to_varchar(date_day, 'Mon YYYY')                              as month_year_label,
        'Q' || date_part('quarter', date_day)
            || ' ' || date_part('year', date_day)                     as quarter_label

    from date_spine

)

select * from final
