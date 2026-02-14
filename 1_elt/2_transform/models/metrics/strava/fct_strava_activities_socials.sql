with
strava_activities as (
    select * from {{ ref('int_strava_activities') }}
),

final as (
    select

        strava_activity_id,
        strava_activity_id_of_day,
        date_day,
        name,
        type,
        activity_name,
        is_sport_exercise,

        -- social metrics
        achievement_count,
        kudos_count,
        comment_count,
        athlete_count,

        -- settings
        trainer,
        commute,
        manual,
        private,
        flagged,
        gear_id

    from strava_activities
)

select * from final
