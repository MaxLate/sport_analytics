{% test end_date_warning(model) %}

-- Warns when today's date is >= your model's end_date expression (within 3 days of 4 years from now)
with params as (
    select date_add(current_date, INTERVAL 4 YEAR) as end_date
)

select 1
from params
where current_date >= end_date - interval '3' day
  and current_date < end_date

{% endtest %}

