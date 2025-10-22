WITH stg_repairs AS (
    SELECT * FROM {{ ref('stg_autoglass_repairs') }}
)
SELECT
    -- Create a unique surrogate key for each distinct combination of the columns below.
    {{ dbt_utils.generate_surrogate_key(['vehicle_brand', 'vehicle_model', 'vehicle_type', 'vehicle_age_in_years']) }} AS vehicle_key,
    
    vehicle_brand,
    vehicle_model,
    vehicle_type,
    vehicle_age_in_years

FROM stg_repairs

    WHERE vehicle_brand IS NOT NULL
      AND vehicle_model IS NOT NULL
      AND vehicle_type IS NOT NULL
      AND vehicle_age_in_years IS NOT NULL
GROUP BY
    
    2, 3, 4, 5
    


