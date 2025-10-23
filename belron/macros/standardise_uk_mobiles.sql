{% macro standardise_uk_mobiles(column_name) %}
    CASE
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE({{ column_name }}, r'[^0-9]', ''), r'^07\d{9}$')
            THEN CONCAT('+44', SUBSTR(REGEXP_REPLACE({{ column_name }}, r'[^0-9]', ''), 2))
        WHEN REGEXP_CONTAINS(REGEXP_REPLACE({{ column_name }}, r'[^0-9]', ''), r'^447\d{9}$')
            THEN CONCAT('+', REGEXP_REPLACE({{ column_name }}, r'[^0-9]', ''))
        ELSE NULL
    END
{% endmacro %}