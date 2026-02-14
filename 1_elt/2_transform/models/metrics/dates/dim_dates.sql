-- i started to track my activities with strava first, so i need to track dates since then
{% set min_date %}
    select min(date_day) from {{ ref('fct_strava_activities') }}
{% endset %}

with

date_spine as (  -- noqa: disable=all
    {{ date_spine(
        start_date = "(" ~ min_date ~ ")",
        datepart = "day",
        end_date = "date_add(current_date, INTERVAL 4 YEAR)"
    ) }}
),

final as (

    select

        date_day :: date as date_day,

        date_part('year', date_day) as year,
        date_part('month', date_day) as month,
        date_part('week', date_day) as week,
        date_part('day', date_day) as day,

        strftime('%A', date_day) as day_name,
        strftime('%B', date_day) as month_name,
        strftime('%w', date_day) as weekday_number,

        weekday_number in (1, 2, 3, 4, 5) as is_weekday,
        weekday_number in (0, 6) as is_weekend,

        row_number() over (partition by year order by date_day) as day_of_year

    from date_spine
)

select * from final
