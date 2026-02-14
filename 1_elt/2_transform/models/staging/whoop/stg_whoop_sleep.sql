with

whoop_sleep_data as (
    select * from {{ source('whoop', 'whoop_sleeps') }}
),

final as (
    select

        "id" :: timestamp as sleep_id,
        "start" :: timestamp as sleep_onset,
        "end" :: timestamp as wake_onset,
        "score".sleep_performance_percentage :: decimal(10, 2) as sleep_performance,
        "score".respiratory_rate :: decimal(10, 2) as respiratory_rate,

        -- Calculate asleep duration as sum of all sleep stages (in minutes)
        (
            score.stage_summary.total_light_sleep_time_milli
            + score.stage_summary.total_slow_wave_sleep_time_milli
            + score.stage_summary.total_rem_sleep_time_milli
        ) / 60000.0 :: decimal(10, 2) as asleep_duration,

        -- Convert milliseconds to minutes
        score.stage_summary.total_in_bed_time_milli / 60000.0 :: decimal(10, 2) as in_bed_duration,
        score.stage_summary.total_light_sleep_time_milli / 60000.0 :: decimal(10, 2) as light_sleep_duration,
        score.stage_summary.total_slow_wave_sleep_time_milli / 60000.0 :: decimal(10, 2) as deep_sleep_duration,
        score.stage_summary.total_rem_sleep_time_milli / 60000.0 :: decimal(10, 2) as rem_duration,
        score.stage_summary.total_awake_time_milli / 60000.0 :: decimal(10, 2) as awake_duration,

        -- Calculate total sleep need (in minutes)
        (
            score.sleep_needed.baseline_milli
            + score.sleep_needed.need_from_sleep_debt_milli
            + score.sleep_needed.need_from_recent_strain_milli
            - score.sleep_needed.need_from_recent_nap_milli
        ) / 60000.0 :: decimal(10, 2) as sleep_need,

        -- Sleep debt is the need from sleep debt (in minutes)
        score.sleep_needed.need_from_sleep_debt_milli / 60000.0 :: decimal(10, 2) as sleep_debt,
        score.sleep_efficiency_percentage :: decimal(10, 2) as sleep_efficiency,
        score.sleep_consistency_percentage :: decimal(10, 2) as sleep_consistency,
        nap :: boolean as nap

    from whoop_sleep_data
)

select * from final
