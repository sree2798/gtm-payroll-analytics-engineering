select
  contract_id,
  account_id,
  start_date,
  end_date,
  product_bundle,
  arr
from {{ ref('stg_billing_contracts') }}
