-- models/marts/dim_technician.sql

-- This model creates a dimension table for unique technicians.
-- It reads from the clean staging data, de-duplicates, and creates a surrogate key.

WITH stg_repairs AS (
    SELECT * FROM {{ ref('stg_autoglass_repairs') }}
)

SELECT
    -- Create a unique surrogate key for each unique technician.
    -- We use the technician_id as the business key.
    {{ dbt_utils.generate_surrogate_key(['technician_id']) }} AS technician_key,
    
    technician_id,
    technician_prefix,
    technician_first_name,
    technician_surname,
    technician_mobile
    
FROM stg_repairs


WHERE technician_id IS NOT NULL


GROUP BY
    2, 3, 4, 5, 6 -- Group by all the descriptive columns

