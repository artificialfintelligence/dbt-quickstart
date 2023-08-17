with

source as (
    select * from {{ source('stripe', 'payment') }}
),
transformed as (
    select
        id as payment_id,
        orderid as order_id,
        paymentmethod as payment_method,
        status as payment_status,
        {{ cents_to_dollars("amount") }} as payment_amount,
        created as payment_created_at
    from {{ source("stripe", "payment") }}
    -- {{ limit_data_in_dev("created", 365) }}
)

select * from transformed