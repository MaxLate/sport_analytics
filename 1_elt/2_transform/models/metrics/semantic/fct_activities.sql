with

strava as (
    select * from {{ ref('fct_strava_activities') }}
),

activity_type as (
    select * from {{ ref('dim_strava_activity_type') }}
),

final as (

    select
        strava.strava_activity_id,
        strava.strava_activity_id_of_day,
        strava.date_day,
        strava.name,
        activity_type.activity_name,
        activity_type.is_sport_exercise

    from strava
    left join activity_type on strava.strava_activity_type_id = activity_type.strava_activity_type_id
)

select * from final
