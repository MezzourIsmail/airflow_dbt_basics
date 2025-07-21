with
    sessions as (select * from {{ ref('int_orders_payments') }}),

    revenue as (
        select
            customer_id,
            count(distinct order_id) as total_orders,
            sum(amount) as total_revenue
        from sessions
        group by customer_id
    )

select c.customer_id, c.first_name, c.last_name, r.total_orders, r.total_revenue
from revenue r
join {{ ref('stg_customers') }} c on r.customer_id = c.customer_id
