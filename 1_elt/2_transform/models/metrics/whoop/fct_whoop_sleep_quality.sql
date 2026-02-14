with

whoop_quality as (
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

        -- sleep quality
        sleep_performance,
        respiratory_rate,
        sleep_need_minutes,
        sleep_need_hours,
        sleep_debt_minutes,
        sleep_debt_hours,
        sleep_efficiency,
        sleep_consistency,
        nap,

        -- moon data
        moon_phase,
        moon_phase_cycle,
        moon_illumination

    from whoop_quality
)

select * from final
