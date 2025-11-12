{% macro test_unmapped_values(model, column_name, allowed_values) %}

WITH invalid AS (
    SELECT {{ column_name }}
    FROM {{ model }}
    WHERE {{ column_name }} NOT IN (
        {%- for val in allowed_values -%}
            '{{ val }}'{% if not loop.last %}, {% endif %}
        {%- endfor -%}
    )
    AND {{ column_name }} IS NOT NULL
)
SELECT * FROM invalid

{% endmacro %}


