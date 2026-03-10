-- Kimball fact table: one row per Strava activity
-- Foreign keys: date_day -> dim_dates, strava_activity_type_id -> dim_strava_activity_type, strava_gear_id -> dim_strava_gear
-- Degenerate dimensions: strava_activity_id, strava_activity_id_of_day, name (activity title)
with

strava as (
    select * from {{ ref('int_strava_activities') }}
),

activity_type as (
    select * from {{ ref('dim_strava_activity_type') }}
),

gear as (
    select * from {{ ref('dim_strava_gear') }}
),

final as (

    select
        -- Degenerate dimensions (natural key / context kept in fact)
        strava.strava_activity_id,
        strava.strava_activity_id_of_day,
        strava.name,

        -- Foreign keys to dimensions
        strava.date_day,
        activity_type.strava_activity_type_id,
        gear.strava_gear_id,

        -- Activity metrics (measures)
        strava.distance_meters,
        strava.distance_kilometers,
        strava.moving_time_seconds,
        strava.moving_time_minutes,
        strava.moving_time_hours,
        strava.elapsed_time_seconds,
        strava.elapsed_time_minutes,
        strava.elapsed_time_hours,
        strava.total_elevation_gain_meters,
        strava.average_speed_meters_per_second,
        strava.average_speed_kilometers_per_hour,
        strava.max_speed_meters_per_second,
        strava.max_speed_kilometers_per_hour,

        -- Location (degenerate / context)
        strava.start_latitude,
        strava.start_longitude,
        strava.end_latitude,
        strava.end_longitude,

        -- Health metrics (measures)
        strava.average_heartrate,
        strava.max_heartrate,
        strava.average_watts,
        strava.kilojoules,
        strava.average_cadence

    from strava
    left join activity_type
    on strava.type = activity_type.type
        and strava.activity_name = activity_type.activity_name
        and strava.is_sport_exercise = activity_type.is_sport_exercise
    left join gear on strava.gear_id is not distinct from gear.gear_id

)

select * from final
