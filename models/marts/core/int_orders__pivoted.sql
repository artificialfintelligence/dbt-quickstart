{%- set order_statuses = ["placed", "shipped", "completed", "return_pending", "returned"] -%}

with orders as (
    select * from {{ ref("stg_orders") }}
),

pivoted as (
    select 
        customer_id,
        {%- for order_status in order_statuses -%}
        sum(case when status = "{{ order_status }}" then 1 else 0 end) as {{ order_status }}_count
        {%- if not loop.last -%}
        ,
        {% endif -%}
        {% endfor %}
    from orders
    group by customer_id
)

select * from pivoted