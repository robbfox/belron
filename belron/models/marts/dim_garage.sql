-- models/marts/dim_garage.sql

WITH stg_repairs AS (
    SELECT * FROM {{ ref('stg_autoglass_repairs') }}
)

SELECT
    -- Create a unique surrogate key for each distinct combination of the columns below.
    -- Now correctly includes garage_type.
    {{ dbt_utils.generate_surrogate_key(['garage_name', 'city', 'region', 'garage_type']) }} AS garage_key,
    
    garage_name,
    city,
    region,
    garage_type
    
FROM stg_repairs

-- Ensure we have a WHERE clause to exclude records where the business key is incomplete
WHERE garage_name IS NOT NULL
  AND city IS NOT NULL
  AND region IS NOT NULL

GROUP BY
    -- The numbers refer to the position of the columns in the SELECT statement.
    -- We are grouping by all descriptive columns to get our unique dimension rows.
    2, 3, 4, 5