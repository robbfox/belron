SELECT
    COUNT(DISTINCT Job_ID) AS number_of_IDS
FROM {{ source('raw_repairs_data', 'raw_repairs') }}
