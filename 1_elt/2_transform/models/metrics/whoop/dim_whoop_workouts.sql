with

whoop_workouts as (
    select * from {{ ref('fct_whoop_workouts') }}
),

filter_activities as (

    select distinct
        activity_name,
        is_sport_exercise

    from whoop_workouts
    order by 1
),

enhanced as (
    select
        row_number() over (order by activity_name) as whoop_workout_activity_name_id,
        activity_name,
        is_sport_exercise

    from filter_activities
)

select * from enhanced
