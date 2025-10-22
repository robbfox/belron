-- models/marts/fact_repairs.sql

-- This model creates the central fact table by joining the staging data
-- with all the final dimension tables to look up the surrogate keys.

-- CTE for the clean staging data (our "ledger")
WITH stg_repairs AS (
    SELECT * FROM {{ ref('stg_autoglass_repairs') }}
),

-- CTEs for each dimension table (our "contact lists")
dim_garage AS (
    SELECT * FROM {{ ref('dim_garage') }}
),

dim_customer AS (
    SELECT * FROM {{ ref('dim_customer') }}
),

dim_technician AS (
    SELECT * FROM {{ ref('dim_technician') }}
),

dim_vehicle AS (
    SELECT * FROM {{ ref('dim_vehicle') }}
),

dim_date AS (
    SELECT * FROM {{ ref('dim_date') }}
),

dim_repair_details AS (
    SELECT * FROM {{ ref('dim_repair_details') }}
),

dim_job_context AS (
    SELECT * FROM {{ ref('dim_job_context') }}
)

-- Final SELECT statement to build the fact table
SELECT
    -- Surrogate Keys from each dimension
    dg.garage_key,
    dc.customer_key,
    dt.technician_key,
    dv.vehicle_key,
    dd.date_key,
    drd.repair_details_key,
    djc.job_context_key,
    
    -- Degenerate Dimension
    stg.job_id,
    
    -- Numeric Measures (The Facts)
    stg.repair_cost,
    stg.glass_cost,
    stg.profit,
    stg.job_duration_in_hours,
    stg.customer_rating
    
FROM stg_repairs AS stg

-- Join to each dimension table to look up the surrogate key
LEFT JOIN dim_garage AS dg
    ON stg.garage_name = dg.garage_name
    AND stg.city = dg.city
    AND stg.region = dg.region
    AND stg.garage_type = dg.garage_type

LEFT JOIN dim_customer AS dc
    ON stg.customer_id = dc.customer_id

LEFT JOIN dim_technician AS dt
    ON stg.technician_id = dt.technician_id

LEFT JOIN dim_vehicle AS dv
    ON stg.vehicle_brand = dv.vehicle_brand
    AND stg.vehicle_model = dv.vehicle_model
    AND stg.vehicle_type = dv.vehicle_type
    AND stg.vehicle_age_in_years = dv.vehicle_age_in_years
    
LEFT JOIN dim_date AS dd
    ON stg.repair_date = dd.full_date

LEFT JOIN dim_repair_details AS drd
    ON stg.damage_type = drd.damage_type
    AND stg.repair_type = drd.repair_type
    AND stg.glass_type = drd.glass_type
    AND stg.window_position = drd.window_position

LEFT JOIN dim_job_context AS djc
    ON stg.insurance_claimed = djc.insurance_claimed
    AND stg.weather_condition = djc.weather_condition
    AND stg.traffic_level = djc.traffic_level