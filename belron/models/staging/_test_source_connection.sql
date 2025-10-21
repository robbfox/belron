-- This is a temporary model to test the connection to our raw data source.
-- It simply counts the number of rows in the raw_repairs table.

SELECT
    COUNT(*) AS number_of_rows
FROM {{ source('raw_repairs_data', 'raw_repairs') }}