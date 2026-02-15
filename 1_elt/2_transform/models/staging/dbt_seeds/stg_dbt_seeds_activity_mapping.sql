with

activity_mapping_data as (
    select * from {{ ref('activity_mapping') }}
),

final as (

    select

        "activity_name" :: varchar as original_activity_name,
        "aligned_activity_name" :: varchar as aligned_activity_name,
        "is_sport_exercise" :: boolean as is_sport_exercise

    from activity_mapping_data

)

select * from final
