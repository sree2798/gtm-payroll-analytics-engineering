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

  date_diff(date(closed_at), date(created_at), day) as sales_cycle_days,
  cast(amount_arr * (1 - discount_pct) as int64) as net_arr_after_discount,

  -- synthetic rep assignment (stable + deterministic)
  1 + mod(abs(farm_fingerprint(opp_id)), 15) as rep_id
from {{ ref('stg_crm__opportunities') }}
