select
  usage_date,
  account_id,
  payroll_runs,
  active_admins,
  adopt_time,
  adopt_benefits,
  adopt_full_hcm
from {{ ref('stg_product_usage_daily') }}
