{% macro employee_band_id(employee_count_col) %}
(
  select band_id
  from {{ ref('employee_bands') }}
  where {{ employee_count_col }} >= min_employees
    and (max_employees is null or {{ employee_count_col }} <= max_employees)
  limit 1
)
{% endmacro %}
