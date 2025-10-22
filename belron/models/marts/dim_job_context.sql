-- models/marts/dim_garage.sql

WITH stg_repairs AS (
    SELECT * FROM {{ ref('stg_autoglass_repairs') }}
)

SELECT
    -- Create a unique surrogate key for each distinct combination of the columns below.
    -- Now correctly includes garage_type.
    {{ dbt_utils.generate_surrogate_key(['insurance_claimed', 'weather_condition', 'traffic_level']) }} AS job_context_key,
    
    insurance_claimed,
    weather_condition,
    traffic_level
    
FROM stg_repairs

-- Ensure we have a WHERE clause to exclude records where the business key is incomplete
WHERE insurance_claimed IS NOT NULL
  AND weather_condition IS NOT NULL
  AND traffic_level IS NOT NULL
GROUP BY
    -- The numbers refer to the position of the columns in the SELECT statement.
    -- We are grouping by all descriptive columns to get our unique dimension rows.
    2, 3, 4