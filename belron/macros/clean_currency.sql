{% macro clean_currency(column_name) %}
    ROUND(
        SAFE_CAST(
            NULLIF(
                REGEXP_REPLACE({{ column_name }}, r'[^0-9.]', ''),
                ''
            ) AS FLOAT64
        ),
        2
    )
{% endmacro %}
