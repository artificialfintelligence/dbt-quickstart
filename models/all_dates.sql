{{ config(materialized="table") }}

{{
    dbt_utils.date_spine(
        datepart="day",
        start_date="parse_date('%Y/%m/%d', '2020/01/01')",
        end_date="parse_date('%Y/%m/%d', '2021/01/01')",
    )
}}
