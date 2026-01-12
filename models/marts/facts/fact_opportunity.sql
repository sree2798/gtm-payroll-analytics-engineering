select
  opp_id,
  account_id,
  created_at,
  closed_at,
  date(created_at) as created_date,
  date(closed_at) as closed_date,
  amount_arr,
  discount_pct,
  is_won,
  stage,
  primary_competitor,
  source_channel,
  -- useful derived metrics
  date_diff(date(closed_at), date(created_at), day) as sales_cycle_days,
  cast(amount_arr * (1 - discount_pct) as int64) as net_arr_after_discount
from {{ ref('stg_crm__opportunities') }}
