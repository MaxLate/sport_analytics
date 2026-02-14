with

whoop_sleeps as (
    select * from {{ ref('int_whoop_sleep') }}
),

final as (
    select

        sleep_id,
        sleep_id_of_day,
        date_day,
        sleep_start_date,
        sleep_onset,
        wake_up_date,
        wake_onset,

        -- sleep duration
        asleep_duration_minutes,
        asleep_duration_hours,
        in_bed_duration_minutes,
        in_bed_duration_hours,
        light_sleep_duration_minutes,
        light_sleep_duration_hours,
        deep_sleep_duration_minutes,
        deep_sleep_duration_hours,
        rem_duration_minutes,
        rem_duration_hours,
        awake_duration_minutes,
        awake_duration_hours,

        -- moon data
        moon_phase,
        moon_phase_cycle,
        moon_illumination

    from whoop_sleeps
)

select * from final
