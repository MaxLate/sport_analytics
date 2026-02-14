with

whoop_physiological_cycles_data as (
    select * from {{ source('whoop', 'whoop_physiological_cycles') }}
),

final as (
    select

        "Cycle start time" :: timestamp as cycle_start_time,
        "Cycle end time" :: timestamp as cycle_end_time,
        "Cycle timezone" :: varchar as cycle_timezone,
        "Recovery score %" :: decimal(10, 2) as recovery_score,
        "Resting heart rate (bpm)" :: decimal(10, 2) as resting_heart_rate,
        "Heart rate variability (ms)" :: decimal(10, 2) as heart_rate_variability,
        "Skin temp (celsius)" :: decimal(10, 2) as skin_temp,
        "Blood oxygen %" :: decimal(10, 2) as blood_oxygen,
        "Day Strain" :: decimal(10, 2) as day_strain,
        "Energy burned (cal)" :: decimal(10, 2) as energy_burned,
        "Max HR (bpm)" :: decimal(10, 2) as max_hr,
        "Average HR (bpm)" :: decimal(10, 2) as average_hr,
        "Sleep onset" :: timestamp as sleep_onset,
        "Wake onset" :: timestamp as wake_onset,
        "Sleep performance %" :: decimal(10, 2) as sleep_performance,
        "Respiratory rate (rpm)" :: decimal(10, 2) as respiratory_rate,
        "Asleep duration (min)" :: decimal(10, 2) as asleep_duration,
        "In bed duration (min)" :: decimal(10, 2) as in_bed_duration,
        "Light sleep duration (min)" :: decimal(10, 2) as light_sleep_duration,
        "Deep (SWS) duration (min)" :: decimal(10, 2) as deep_sleep_duration,
        "REM duration (min)" :: decimal(10, 2) as rem_duration,
        "Awake duration (min)" :: decimal(10, 2) as awake_duration,
        "Sleep need (min)" :: decimal(10, 2) as sleep_need,
        "Sleep debt (min)" :: decimal(10, 2) as sleep_debt,
        "Sleep efficiency %" :: decimal(10, 2) as sleep_efficiency,
        "Sleep consistency %" :: decimal(10, 2) as sleep_consistency

    from whoop_physiological_cycles_data
)

select * from final
