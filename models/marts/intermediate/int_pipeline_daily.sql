with o as (
  select * from {{ ref('fact_opportunity') }}
),

created as (
  select
    created_date as date_day,
    account_id,
    sum(amount_arr) as pipeline_created_arr
  from o
  group by 1,2
),

won as (
  select
    closed_date as date_day,
    account_id,
    sum(case when is_won then net_arr_after_discount else 0 end) as won_arr
  from o
  group by 1,2
)

select
  coalesce(c.date_day, w.date_day) as date_day,
  coalesce(c.account_id, w.account_id) as account_id,
  coalesce(c.pipeline_created_arr, 0) as pipeline_created_arr,
  coalesce(w.won_arr, 0) as won_arr
from created c
full outer join won w
  on c.date_day = w.date_day
 and c.account_id = w.account_id
