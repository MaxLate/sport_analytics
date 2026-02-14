{% test end_date_alert(model) %}

-- Fails when today's date is >= your model's end_date expression (4 years from now)
with params as (
    select date_add(current_date, INTERVAL 4 YEAR) as end_date
)

select 1
from params
where current_date >= end_date

{% endtest %}

