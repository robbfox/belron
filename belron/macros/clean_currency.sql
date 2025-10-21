{% macro clean_currency(column_name) %}
    SAFE_CAST(
        NULLIF(
            REGEXP_REPLACE(LOWER({{ column_name }}), r'[^0-9\.-]', ''),
            ''
        ) AS FLOAT64
    )
{% endmacro %}