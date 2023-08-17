{%- set order_statuses = ["placed", "shipped", "completed", "return_pending", "returned"] -%}

with orders as (
    select * from {{ ref("stg_jaffle_shop__orders") }}
),

pivoted as (
    select 
        customer_id,
        {%- for status in order_statuses -%}
        sum(case when order_status = "{{ status }}" then 1 else 0 end) as {{ status }}_count
        {%- if not loop.last -%}
        ,
        {% endif -%}
        {% endfor %}
    from orders
    group by customer_id
)

select * from pivoted