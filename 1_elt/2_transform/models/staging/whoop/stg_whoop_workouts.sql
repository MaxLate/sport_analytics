with

whoop_workouts_data as (
    select * from {{ source('whoop', 'whoop_workouts') }}
),

final as (
    select

        "id" :: varchar as whoop_workout_id,
        "start" :: timestamp as workout_start_time,
        "end" :: timestamp as workout_end_time,
        "sport_name" :: varchar as whoop_activity_name,

        "score".strain :: double as activity_strain,
        "score".average_heart_rate :: integer as average_heart_rate,
        "score".max_heart_rate :: integer as max_heart_rate,
        "score".kilojoule :: double as kilojoule,
        "score".percent_recorded :: double as percent_recorded,
        "score".distance_meter :: double as distance_meter,
        "score".altitude_gain_meter :: double as altitude_gain_meter,
        "score".altitude_change_meter :: double as altitude_change_meter,

        "score".zone_durations.zone_zero_milli :: bigint as zone_zero_milli,
        "score".zone_durations.zone_one_milli :: bigint as zone_one_milli,
        "score".zone_durations.zone_two_milli :: bigint as zone_two_milli,
        "score".zone_durations.zone_three_milli :: bigint as zone_three_milli,
        "score".zone_durations.zone_four_milli :: bigint as zone_four_milli,
        "score".zone_durations.zone_five_milli :: bigint as zone_five_milli,
        "sport_id" :: integer as sport_id

    from whoop_workouts_data
)

select * from final
