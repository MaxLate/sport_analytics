-- Kimball fact table: one row per Strava activity (social/engagement grain)
-- Foreign keys: date_day -> dim_dates, strava_activity_type_id -> dim_strava_activity_type, strava_gear_id -> dim_strava_gear
-- Degenerate dimensions: strava_activity_id, strava_activity_id_of_day, name; flags: trainer, commute, manual, private, flagged
with

strava as (
    select * from {{ ref('int_strava_activities') }}
),

activity_type as (
    select * from {{ ref('dim_strava_activity_type') }}
),

gear as (
    select * from {{ ref('dim_strava_gear') }}
),

final as (

    select
        -- Degenerate dimensions (natural key / context kept in fact)
        strava.strava_activity_id,
        strava.strava_activity_id_of_day,
        strava.name,

        -- Foreign keys to dimensions
        strava.date_day,
        activity_type.strava_activity_type_id,
        gear.strava_gear_id,

        -- Social metrics (measures)
        strava.achievement_count,
        strava.kudos_count,
        strava.comment_count,
        strava.athlete_count,

        -- Degenerate dimensions (flags / context at fact grain)
        strava.trainer,
        strava.commute,
        strava.manual,
        strava.private,
        strava.flagged

    from strava
    left join activity_type on strava.type = activity_type.type
        and strava.activity_name = activity_type.activity_name
        and strava.is_sport_exercise = activity_type.is_sport_exercise
    left join gear on strava.gear_id is not distinct from gear.gear_id

)

select * from final
