with

whoop_workouts_data as (
    select * from {{ ref('stg_whoop_workouts') }}
),

activity_mapping_data as (
    select * from {{ ref('stg_dbt_seeds_activity_mapping') }}
),

enhanced as (
    select

        row_number() over (order by workout_start_time) as whoop_workout_id,
        row_number() over (partition by date_trunc('day', workout_start_time) order by workout_start_time) as whoop_workout_id_of_day,

        date_trunc('day', workout_start_time) :: date as date_day,
        workout_start_time,
        workout_end_time,
        date_diff('minute', workout_start_time, workout_end_time) as duration_minutes,

        whoop_activity_name,
        activity_strain,
        average_heart_rate,
        max_heart_rate,
        kilojoule,
        percent_recorded,
        distance_meter,
        altitude_gain_meter,
        altitude_change_meter,
        zone_zero_milli,
        zone_one_milli,
        zone_two_milli,
        zone_three_milli,
        zone_four_milli,
        zone_five_milli

    from whoop_workouts_data
),

final as (
    select

        -- DuckDB does not support except clause, so we need to list all columns explicitly
        enhanced.whoop_workout_id,
        enhanced.whoop_workout_id_of_day,
        enhanced.date_day,
        enhanced.workout_start_time,
        enhanced.workout_end_time,
        enhanced.duration_minutes,
        enhanced.activity_strain,
        enhanced.average_heart_rate,
        enhanced.max_heart_rate,
        enhanced.kilojoule,
        enhanced.percent_recorded,
        enhanced.distance_meter,
        enhanced.altitude_gain_meter,
        enhanced.altitude_change_meter,
        enhanced.zone_zero_milli,
        enhanced.zone_one_milli,
        enhanced.zone_two_milli,
        enhanced.zone_three_milli,
        enhanced.zone_four_milli,
        enhanced.zone_five_milli,
        activity_mapping_data.aligned_activity_name as activity_name,
        activity_mapping_data.is_sport_exercise as is_sport_exercise

    from enhanced
    left join activity_mapping_data on enhanced.whoop_activity_name = activity_mapping_data.original_activity_name
)

select * from final
