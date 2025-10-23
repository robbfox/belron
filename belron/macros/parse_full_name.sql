{% macro parse_full_name(column_name) %}
STRUCT(
    -- Extract prefix
    CASE
        WHEN UPPER(SPLIT(TRIM({{ column_name }}), ' ')[SAFE_OFFSET(0)]) IN ('MR','MISS', 'MS', 'MRS', 'DR', 'PROF')
        THEN INITCAP(SPLIT(TRIM({{ column_name }}), ' ')[SAFE_OFFSET(0)])
        ELSE NULL
    END AS prefix,
 
    -- Extract first name
    SPLIT(
        CASE
            WHEN UPPER(SPLIT(TRIM({{ column_name }}), ' ')[SAFE_OFFSET(0)]) IN ('MR', 'MISS', 'MS', 'MRS', 'DR', 'PROF')
            THEN ARRAY_TO_STRING(
                ARRAY(
                    SELECT name_part
                    FROM UNNEST(SPLIT(TRIM({{ column_name }}), ' ')) AS name_part WITH OFFSET i
                    WHERE i > 0
                ),
                ' '
            )
            ELSE TRIM({{ column_name }})
        END,
        ' '
    )[SAFE_OFFSET(0)] AS first_name,
 
    -- Extract surname
    NULLIF(
        ARRAY_TO_STRING(
            ARRAY(
                SELECT surname_part
                FROM UNNEST(SPLIT(
                    CASE
                        WHEN UPPER(SPLIT(TRIM({{ column_name }}), ' ')[SAFE_OFFSET(0)]) IN ('MR', 'MISS', 'MS', 'MRS', 'DR', 'PROF')
                        THEN ARRAY_TO_STRING(
                            ARRAY(
                                SELECT name_part
                                FROM UNNEST(SPLIT(TRIM({{ column_name }}), ' ')) AS name_part WITH OFFSET i
                                WHERE i > 0
                            ),
                            ' '
                        )
                        ELSE TRIM({{ column_name }})
                    END,
                    ' '
                )) AS surname_part WITH OFFSET i
                WHERE i > 0
            ),
            ' '
        ),
        ''
    ) AS surname
)
{% endmacro %}