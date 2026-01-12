select
  band_id,
  band_label,
  cast(min_employees as int64) as min_employees,
  cast(max_employees as int64) as max_employees
from {{ ref('employee_bands') }}
