{% set start_date = "2024-01-01" %}
{% set end_date = "2025-12-31" %}

with dates as (
  select d as date_day
  from unnest(generate_date_array(date('{{ start_date }}'), date('{{ end_date }}'))) d
)

select
  date_day,
  extract(year from date_day) as year,
  extract(quarter from date_day) as quarter,
  extract(month from date_day) as month,
  extract(week from date_day) as week,
  extract(day from date_day) as day_of_month,
  format_date('%Y-%m', date_day) as year_month,
  extract(dayofweek from date_day) as day_of_week
from dates
