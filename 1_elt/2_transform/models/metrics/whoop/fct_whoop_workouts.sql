with

whoop_workouts as (
    select * from {{ ref('int_whoop_workouts') }}
),

final as (
    select

        whoop_workout_id,
        whoop_workout_id_of_day,
        date_day,

        -- workout time metrics
        workout_start_time,
        workout_end_time,
        duration_minutes,

        -- workout activity metrics
        activity_name,
        is_sport_exercise,
        activity_strain,
        kilojoule,

        -- workout heart rate metrics
        max_heart_rate,
        average_heart_rate,
        zone_zero_milli,
        zone_one_milli,
        zone_two_milli,
        zone_three_milli,
        zone_four_milli,
        zone_five_milli,

        -- workout GPS metrics
        percent_recorded,
        distance_meter,
        altitude_gain_meter,
        altitude_change_meter

    from whoop_workouts
)

select * from final
