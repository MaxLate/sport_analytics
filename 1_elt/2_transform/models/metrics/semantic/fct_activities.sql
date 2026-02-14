with

strava_activities as (
    select * from {{ ref('fct_strava_activities') }}
),


final as (

    select
        strava_activity_id,
        strava_activity_id_of_day,
        date_day,
        name,
        activity_name,
        is_sport_exercise

    from strava_activities
)

select * from final
