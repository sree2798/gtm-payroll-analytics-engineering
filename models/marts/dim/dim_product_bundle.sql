select distinct
  product_bundle
from {{ ref('stg_billing_contracts') }}
where product_bundle is not null
