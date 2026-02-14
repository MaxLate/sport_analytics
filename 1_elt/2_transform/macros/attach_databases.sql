{% macro attach_databases() %}
{% if target.name == 'transform' %}
        -- Attach source database for transform target
        -- Path is relative to dbt project directory (transform/dbt_model/) - go up 2 levels to project root
        ATTACH '../../data/database/source.duckdb' AS source (TYPE DUCKDB);
    {% elif target.name == 'analytics' %}
        -- Attach source and transform databases for analytics target
        -- Paths are relative to dbt project directory (transform/dbt_model/) - go up 2 levels to project root
        ATTACH '../../data/database/source.duckdb' AS source (TYPE DUCKDB);
        ATTACH '../../data/database/transform.duckdb' AS transform (TYPE DUCKDB);
    {% endif %}
{% endmacro %}

