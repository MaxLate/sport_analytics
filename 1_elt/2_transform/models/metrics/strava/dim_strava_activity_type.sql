-- Kimball dimension: one row per activity type (type + activity_name + is_sport_exercise)
with

strava_activities as (
    select * from {{ ref('int_strava_activities') }}
),

distinct_activity_types as (
    select distinct
        type,
        activity_name,
        is_sport_exercise
    from strava_activities
    where type is not null
        and activity_name is not null
),

final as (
    select
        row_number() over (order by type, activity_name) as strava_activity_type_id,
        type,
        activity_name,
        is_sport_exercise
    from distinct_activity_types
)

select * from final
