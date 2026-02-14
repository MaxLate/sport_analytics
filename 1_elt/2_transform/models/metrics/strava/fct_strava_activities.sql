with

strava_activities as (
    select * from {{ ref('int_strava_activities') }}
),

final as (
    select

        strava_activity_id,
        strava_activity_id_of_day,
        date_day,

        name,
        type,
        activity_name,
        is_sport_exercise,

        -- activity metrics
        distance_meters,
        distance_kilometers,
        moving_time_seconds,
        moving_time_minutes,
        moving_time_hours,
        elapsed_time_seconds,
        elapsed_time_minutes,
        elapsed_time_hours,
        total_elevation_gain_meters,
        average_speed_meters_per_second,
        average_speed_kilometers_per_hour,
        max_speed_meters_per_second,
        max_speed_kilometers_per_hour,

        -- location metrics
        start_latitude,
        start_longitude,
        end_latitude,
        end_longitude,

        -- health metrics
        average_heartrate,
        max_heartrate,
        average_watts,
        kilojoules,
        average_cadence


    from strava_activities
)

select * from final
