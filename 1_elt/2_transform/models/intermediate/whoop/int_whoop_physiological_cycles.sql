with

whoop_physiological_cycles_data as (
    select * from {{ ref('stg_whoop_physiological_cycles') }}
),

whoop_sleep_data as (
    select * from {{ ref('int_whoop_sleep') }}
),

enhanced as (
    select

        -- sleep id
        row_number() over (order by sleep_onset) as sleep_id,
        row_number() over (partition by date_trunc('day', sleep_onset) order by sleep_onset) as sleep_id_of_day,

        date_trunc('day', sleep_onset) :: date as sleep_start_date,
        sleep_onset,

        date_trunc('day', wake_onset) :: date as wake_up_date,
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

    from whoop_physiological_cycles_data
),

final as (
    select

        enhanced.*,
        -- Add date day
        -- it can happen that the sleep onset start after 00:00, so we need to subtract 1 day for the night date
        case
            when extract(hour from enhanced.sleep_onset) < 12 and whoop_sleep_data.nap is false then (enhanced.sleep_onset :: date - interval '1 day') :: date
            else enhanced.sleep_onset :: date
        end as date_day

    from enhanced
    left join whoop_sleep_data
    on enhanced.sleep_id = whoop_sleep_data.sleep_id
)

select * from final
