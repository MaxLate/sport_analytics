with

moon_data as (
    select * from {{ ref('mooncalender') }}
),

final as (

    select

        "Datum" :: date as date_day,
        "Wochentag" :: varchar as day_of_week,
        "Monat" :: varchar as month,
        "Mondphase" :: varchar as moon_phase,
        "Beleuchtung" :: decimal(10, 2) as illumination,
        "Phasenzyklus" :: integer as phase_cycle

    from moon_data

)

select * from final
