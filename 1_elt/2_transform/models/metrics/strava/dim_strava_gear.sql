-- Kimball dimension: one row per gear (bike, shoes, etc.) from Strava activities
with

strava as (
    select * from {{ ref('int_strava_activities') }}
),

distinct_gear as (
    select distinct gear_id
    from strava
),

final as (

    select
        row_number() over (order by gear_id nulls last) as strava_gear_id,
        gear_id
    from distinct_gear

)

select * from final
