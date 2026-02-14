with

whoop_physiological_cycles as (
    select * from {{ ref('int_whoop_physiological_cycles') }}
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
        cycle_timezone,

        -- physiological cycle metrics which gets caculated from the sleep data
        recovery_score,
        resting_heart_rate,
        heart_rate_variability,
        skin_temp,
        blood_oxygen,
        respiratory_rate,

        -- physiological cycle metrics which gets caculated from the workout data
        day_strain,
        energy_burned,
        max_hr,
        average_hr

    from whoop_physiological_cycles
)

select * from final
