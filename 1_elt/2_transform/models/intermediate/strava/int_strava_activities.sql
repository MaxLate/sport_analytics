with

strava_activities_data as (
    select * from {{ ref('stg_strava_activities') }}
),

activity_mapping_data as (
    select * from {{ ref('stg_dbt_seeds_activity_mapping') }}
),

enhanced as (
    select

        activity_id as strava_activity_id_internal,
        row_number() over (order by start_date) as strava_activity_id,
        row_number() over (partition by date_trunc('day', start_date) order by start_date) as strava_activity_id_of_day,
        name,
        type,
        sport_type as strava_activity_name,
        date_trunc('day', start_date) :: date as date_day,
        start_date,
        start_date_local,
        timezone,

        -- activity metrics
        distance :: numeric(10, 2) as distance_meters,
        distance / 1000 :: numeric(10, 2) as distance_kilometers,

        moving_time :: numeric(10, 2) as moving_time_seconds,
        moving_time / 60 :: numeric(10, 2) as moving_time_minutes,
        moving_time / 3600 :: numeric(10, 2) as moving_time_hours,

        elapsed_time :: numeric(10, 2) as elapsed_time_seconds,
        elapsed_time / 60 :: numeric(10, 2) as elapsed_time_minutes,
        elapsed_time / 3600 :: numeric(10, 2) as elapsed_time_hours,

        total_elevation_gain :: numeric(10, 2) as total_elevation_gain_meters,

        average_speed :: numeric(10, 2) as average_speed_meters_per_second,
        average_speed * 3.6 :: numeric(10, 2) as average_speed_kilometers_per_hour,

        max_speed :: numeric(10, 2) as max_speed_meters_per_second,
        max_speed * 3.6 :: numeric(10, 2) as max_speed_kilometers_per_hour,

        start_latitude,
        start_longitude,
        end_latitude,
        end_longitude,

        -- health metrics
        average_heartrate,
        max_heartrate,
        average_watts,
        kilojoules,
        average_cadence,

        -- social metrics
        achievement_count,
        kudos_count,
        comment_count,
        athlete_count,

        -- settings
        trainer,
        commute,
        manual,
        private,
        flagged,
        gear_id,

        extracted_at

    from strava_activities_data
),

final as (
    select

        -- DuckDB does not support except clause, so we need to list all columns explicitly
        enhanced.strava_activity_id_internal,
        enhanced.strava_activity_id,
        enhanced.strava_activity_id_of_day,
        enhanced.name,
        enhanced.type,
        enhanced.date_day,
        enhanced.start_date,
        enhanced.start_date_local,
        enhanced.timezone,
        enhanced.distance_meters,
        enhanced.distance_kilometers,
        enhanced.moving_time_seconds,
        enhanced.moving_time_minutes,
        enhanced.moving_time_hours,
        enhanced.elapsed_time_seconds,
        enhanced.elapsed_time_minutes,
        enhanced.elapsed_time_hours,
        enhanced.total_elevation_gain_meters,
        enhanced.average_speed_meters_per_second,
        enhanced.average_speed_kilometers_per_hour,
        enhanced.max_speed_meters_per_second,
        enhanced.max_speed_kilometers_per_hour,
        enhanced.start_latitude,
        enhanced.start_longitude,
        enhanced.end_latitude,
        enhanced.end_longitude,
        enhanced.average_heartrate,
        enhanced.max_heartrate,
        enhanced.average_watts,
        enhanced.kilojoules,
        enhanced.average_cadence,
        enhanced.achievement_count,
        enhanced.kudos_count,
        enhanced.comment_count,
        enhanced.athlete_count,
        enhanced.trainer,
        enhanced.commute,
        enhanced.manual,
        enhanced.private,
        enhanced.flagged,
        enhanced.gear_id,
        enhanced.extracted_at,
        activity_mapping_data.aligned_activity_name as activity_name,
        activity_mapping_data.is_sport_exercise as is_sport_exercise

    from enhanced
    left join activity_mapping_data on enhanced.strava_activity_name = activity_mapping_data.aligned_activity_name
)

select * from final
