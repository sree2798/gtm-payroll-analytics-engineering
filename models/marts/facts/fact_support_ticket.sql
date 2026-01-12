select
  ticket_id,
  account_id,
  created_at,
  date(created_at) as created_date,
  severity,
  resolution_hours,
  csat
from {{ ref('stg_support_tickets') }}
