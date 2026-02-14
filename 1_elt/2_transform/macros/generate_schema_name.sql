{% macro generate_schema_name(custom_schema_name, node) -%}
{#- Organize schemas by entity name (whoop, strava, etc.) -#}
{%- if custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
{#- Determine schema based on model name and path -#}
{%- set model_name_lower = node.name | lower -%}
{%- set model_path = node.path | lower -%}
        
{#- Check for whoop models -#}
{%- if 'whoop' in model_name_lower or 'whoop' in model_path -%}
            whoop
        {#- Check for strava models -#}
        {%- elif 'strava' in model_name_lower or 'strava' in model_path -%}
            strava
        {#- Check for seed models -#}
        {%- elif 'seed' in model_path or 'dbt_seeds' in model_path -%}
            seeds
        {#- Check for dates models -#}
        {%- elif 'date' in model_name_lower or 'dates' in model_path -%}
            dates
        {#- Check for semantic models -#}
        {%- elif 'semantic' in model_path -%}
            semantic
        {#- Default to target schema -#}
        {%- else -%}
{{ target.schema }}
{%- endif -%}
{%- endif -%}
{%- endmacro %}

