with

strava_activities_data as (
    select * from {{ source('strava', 'strava_activities') }}
),

final as (
    select

        activity_id :: bigint as activity_id,
        name :: varchar as name,
        type :: varchar as type,
        sport_type :: varchar as sport_type,
        start_date :: timestamp as start_date,
        start_date_local :: timestamp as start_date_local,
        timezone :: varchar as timezone,
        distance :: double as distance, -- in meters
        moving_time :: integer as moving_time,   -- in seconds
        elapsed_time :: integer as elapsed_time, -- in seconds
        total_elevation_gain :: double as total_elevation_gain, -- in meters
        average_speed :: double as average_speed, -- in meters per second
        max_speed :: double as max_speed, -- in meters per second
        average_heartrate :: double as average_heartrate,
        max_heartrate :: double as max_heartrate,
        average_watts :: double as average_watts,
        kilojoules :: double as kilojoules,
        average_cadence :: double as average_cadence,
        achievement_count :: integer as achievement_count,
        kudos_count :: integer as kudos_count,
        comment_count :: integer as comment_count,
        athlete_count :: integer as athlete_count,
        trainer :: boolean as trainer,
        commute :: boolean as commute,
        manual :: boolean as manual,
        private :: boolean as private,
        flagged :: boolean as flagged,
        gear_id :: varchar as gear_id,
        start_latitude :: double as start_latitude,
        start_longitude :: double as start_longitude,
        end_latitude :: double as end_latitude,
        end_longitude :: double as end_longitude,
        extracted_at :: timestamp as extracted_at

    from strava_activities_data
)

select * from final
