WITH 
stg_repairs AS (
    SELECT * FROM {{ ref('stg_autoglass_repairs') }}
),
date_spine AS (

    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('2020-01-01' as date)",
        end_date="cast('2030-12-31' as date)"
    ) }}
)

SELECT
    -- The Surrogate Key: Format the date as an integer (e.g., 20240527)
    CAST(FORMAT_DATE('%Y%m%d', date_day) AS INT64) AS date_key,
    
    -- Date Attributes
    date_day AS full_date,
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(MONTH FROM date_day) AS month,
    FORMAT_DATE('%B', date_day) AS month_name,
    EXTRACT(DAYOFWEEK FROM date_day) AS day_of_week, -- Sunday=1, Saturday=7
    FORMAT_DATE('%A', date_day) AS day_name,
    
    -- Boolean Flag for Weekend
    CASE
        WHEN FORMAT_DATE('%A', date_day) IN ('Saturday', 'Sunday') THEN TRUE
        ELSE FALSE
    END AS is_weekend

FROM date_spine
