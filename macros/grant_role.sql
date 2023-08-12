{# 
{% macro grant_role(schema=target.schema, role='`roles/bigquery.dataViewer`', user='"user:[user_s_email_address@domain.com]"') %}
#}
{% macro grant_role(schema, role, user) %}

    {% set sql %}
        grant {{ role }} on schema {{ schema }} to {{ user }};
    {% endset %}

    {{
        log(
            "Granting role " ~ role ~ " in schema " ~ schema ~ " to user " ~ user,
            info=True,
        )
    }}

    {% do run_query(sql) %}

    {{ log("Privileges granted!", info=True) }}

{% endmacro %}
