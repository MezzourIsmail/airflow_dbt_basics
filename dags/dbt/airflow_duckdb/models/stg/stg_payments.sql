with src as (
  select * from {{ ref('raw_payments') }}
)

select
  id::int                           as payment_id,
  order_id::int                     as order_id,
  payment_method::varchar           as payment_method,
  amount::decimal(10,2)             as amount
from src
