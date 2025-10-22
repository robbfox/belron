-- models/marts/dim_repair_details.sql

WITH stg_repairs AS (
    SELECT * FROM {{ ref('stg_autoglass_repairs') }}
)

SELECT
    -- Create a unique surrogate key for each distinct combination of the columns below.
    {{ dbt_utils.generate_surrogate_key(['damage_type', 'repair_type', 'glass_type', 'window_position']) }} AS repair_details_key,
    
    damage_type,
    repair_type,
    glass_type,
    window_position

FROM stg_repairs

WHERE
    damage_type IS NOT NULL
    AND repair_type IS NOT NULL
    AND glass_type IS NOT NULL
    AND window_position IS NOT NULL

GROUP BY
    -- The numbers refer to the position of the columns in the SELECT statement.
    2, 3, 4, 5
