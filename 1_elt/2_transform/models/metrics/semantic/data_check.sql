with

dates as (
    select * from {{ ref('dim_dates') }}
),

whoop_sleeps as (
    select * from {{ ref('fct_whoop_sleeps') }}
),

whoop_workouts as (
    select * from {{ ref('fct_whoop_workouts') }}
),

whoop_physiological_cycles as (
    select * from {{ ref('fct_whoop_physiological_cycles') }}
),

strava_activities as (
    select * from {{ ref('fct_strava_activities') }}
),

combined_data as (

    select

        dates.date_day,
        dates.day_name,
        dates.is_weekend,

        -- to avoid duplicates, we use exists instead of left join
        exists(select 1 from whoop_sleeps where whoop_sleeps.date_day = dates.date_day) as is_whoop_sleeps_data_available,
        exists(select 1 from whoop_workouts where whoop_workouts.date_day = dates.date_day) as is_whoop_workouts_data_available,
        exists(select 1 from whoop_physiological_cycles where whoop_physiological_cycles.date_day = dates.date_day) as is_whoop_physiological_cycles_data_available,
        exists(select 1 from strava_activities where strava_activities.date_day = dates.date_day) as is_strava_activities_data_available

    from dates

)

select * from combined_data
