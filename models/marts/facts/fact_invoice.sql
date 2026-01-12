select
  invoice_id,
  account_id,
  contract_id,
  invoice_date,
  amount_usd,
  status
from {{ ref('stg_billing_invoices') }}
