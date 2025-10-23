WITH source AS (
    SELECT * 
    FROM {{ source('raw_repairs_data', 'raw_repairs_data') }}
),

-- Lookup table for missing regions - FIX: Trim the region values here
region_lookup AS (
    SELECT
        LOWER(TRIM(city)) AS city,
        ANY_VALUE(TRIM(region)) AS region  -- Added TRIM here
    FROM source
    WHERE TRIM(region) IS NOT NULL
      AND TRIM(region) != 'N/A'
      AND TRIM(region) != ''
      AND TRIM(city) IS NOT NULL
      AND TRIM(city) != 'N/A'
      AND TRIM(city) != ''
    GROUP BY LOWER(TRIM(city))  -- Added TRIM here too
),

cleaned AS (
    SELECT DISTINCT * 
    FROM (
        SELECT

            CONCAT("JOB", CAST(job_ID AS STRING)) AS Job_ID,

            COALESCE(
                SAFE.PARSE_DATE('%d-%b-%y', repair_date),
                CASE
                    WHEN SAFE_CAST(SPLIT(repair_date, '/')[1] AS INT64) > 12
                        THEN SAFE.PARSE_DATE('%m/%d/%Y', repair_date)
                    ELSE SAFE.PARSE_DATE('%d/%m/%Y', repair_date)
                END
            ) AS repair_date,

            -- FIX: Trim both sources and wrap the entire result
            TRIM(
                INITCAP(
                    COALESCE(
                        NULLIF(TRIM(source.region), 'N/A'),
                        TRIM(rl.region)  -- Added TRIM here
                    )
                )
            ) AS region,

            TRIM(INITCAP(NULLIF(TRIM(source.city), 'N/A'))) AS city,

            TRIM(garage_name) AS garage_name,  -- Added TRIM

            CASE 
                WHEN UPPER(TRIM(Garage_Type)) IN ('FRANCHSIE', 'CHAIN') THEN 'Franchise'
                WHEN UPPER(TRIM(Garage_Type)) = 'INDEPENDENT' THEN 'Independent' 
                WHEN UPPER(TRIM(Garage_Type)) = 'MOBILE' THEN 'Mobile' 
                ELSE NULL 
            END AS garage_type,

            CASE
                WHEN UPPER(TRIM(Vehicle_Brand)) = 'BMW' THEN 'BMW'
                WHEN UPPER(TRIM(Vehicle_Brand)) = 'N/A' THEN NULL
                ELSE INITCAP(TRIM(Vehicle_Brand))
            END AS vehicle_brand,

            CASE
                WHEN UPPER(TRIM(Vehicle_Type)) = 'SUV' THEN 'SUV'
                WHEN UPPER(TRIM(Vehicle_Type)) = 'CAR' THEN 'Car'
                WHEN UPPER(TRIM(Vehicle_Type)) = 'VAN' THEN 'Van'
                WHEN UPPER(TRIM(Vehicle_Type)) = 'TRUCK' THEN 'Truck'
                WHEN UPPER(TRIM(Vehicle_Type)) IN ('MOTORCYCLE', 'MOTORBIKE') THEN 'Motorcycle'
                ELSE NULL
            END AS vehicle_type,

            CASE
                WHEN UPPER(TRIM(Vehicle_Model)) = 'N/A' THEN NULL
                WHEN UPPER(TRIM(Vehicle_Model)) = 'R 1250 GS' THEN 'R 1250 GS'
                ELSE INITCAP(TRIM(Vehicle_Model))
            END AS vehicle_model,

            CASE
                WHEN UPPER(TRIM(Glass_Type)) = 'N/A' THEN NULL
                WHEN UPPER(TRIM(Glass_Type)) = 'OEM' THEN 'OEM'
                ELSE INITCAP(TRIM(Glass_Type))
            END AS glass_type,

            CASE
                WHEN UPPER(TRIM(REPLACE(Window_Position, '_', ' '))) IN ('WINDSHIELD', 'WIND SCREEN')
                    THEN 'Windscreen'
                WHEN UPPER(TRIM(Window_Position)) = 'N/A'
                    THEN NULL
                ELSE INITCAP(TRIM(Window_Position))
            END AS window_position,

            CASE
                WHEN UPPER(TRIM(Damage_Type)) = 'N/A' THEN NULL
                ELSE INITCAP(TRIM(Damage_Type))
            END AS damage_type,

            CASE
                WHEN UPPER(TRIM(Repair_Type)) = 'N/A' THEN NULL
                ELSE INITCAP(TRIM(Repair_Type))
            END AS repair_type,
        
            {{ clean_currency('repair_cost') }} AS repair_cost,
            {{ clean_currency('glass_cost') }} AS glass_cost,
            ROUND({{ clean_currency('repair_cost') }} - {{ clean_currency('glass_cost') }}, 2) AS profit,
        
            CAST(
                CASE
                    WHEN LOWER(TRIM(customer_rating)) IN ('n/a', '') THEN NULL
                    WHEN LOWER(TRIM(customer_rating)) = 'one' THEN 1
                    WHEN LOWER(TRIM(customer_rating)) = 'five' THEN 5
                    ELSE SAFE_CAST(customer_rating AS INT64) 
                END AS INT64
            ) AS customer_rating,
            
            {{ parse_full_name('customer_name') }}.prefix AS customer_prefix,
            {{ parse_full_name('customer_name') }}.first_name AS customer_first_name,
            {{ parse_full_name('customer_name') }}.surname AS customer_surname,
            
            CASE
                WHEN REGEXP_CONTAINS(LOWER(TRIM(customer_email)), r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
                    THEN LOWER(TRIM(customer_email))
                WHEN customer_email LIKE '%gmail.com' AND NOT customer_email LIKE '%@%'
                    THEN REGEXP_REPLACE(LOWER(TRIM(customer_email)), r'(gmail\.com)', r'@\1')
                WHEN customer_email LIKE '%yahoo.co.uk' AND NOT customer_email LIKE '%@%'
                    THEN REGEXP_REPLACE(LOWER(TRIM(customer_email)), r'(yahoo\.co\.uk)', r'@\1')
                WHEN customer_email LIKE '%yahoo.com' AND NOT customer_email LIKE '%@%'
                    THEN REGEXP_REPLACE(LOWER(TRIM(customer_email)), r'(yahoo\.com)', r'@\1')
                WHEN customer_email LIKE '%hotmail.com' AND NOT customer_email LIKE '%@%'
                    THEN REGEXP_REPLACE(LOWER(TRIM(customer_email)), r'(hotmail\.com)', r'@\1')
                WHEN customer_email LIKE '%hotmail.co.uk' AND NOT customer_email LIKE '%@%'
                    THEN REGEXP_REPLACE(LOWER(TRIM(customer_email)), r'(hotmail\.co\.uk)', r'@\1')
                WHEN customer_email LIKE '%outlook.com' AND NOT customer_email LIKE '%@%'
                    THEN REGEXP_REPLACE(LOWER(TRIM(customer_email)), r'(outlook\.com)', r'@\1')
                WHEN customer_email LIKE '%example.com' AND NOT customer_email LIKE '%@%'
                    THEN REGEXP_REPLACE(LOWER(TRIM(customer_email)), r'(example\.com)', r'@\1')
                WHEN customer_email LIKE '%example.net' AND NOT customer_email LIKE '%@%'
                    THEN REGEXP_REPLACE(LOWER(TRIM(customer_email)), r'(example\.net)', r'@\1')
                WHEN customer_email LIKE '%example.org' AND NOT customer_email LIKE '%@%'
                    THEN REGEXP_REPLACE(LOWER(TRIM(customer_email)), r'(example\.org)', r'@\1')
                ELSE NULL
            END AS customer_email,

            {{ standardise_uk_mobiles('customer_mobile') }} AS customer_mobile,
            
            CASE
                WHEN UPPER(TRIM(customer_postcode)) IN ('N/A', '') THEN NULL
                ELSE UPPER(TRIM(customer_postcode))
            END AS customer_postcode,

            CASE
                WHEN LOWER(TRIM(insurance_claimed)) IN ('yes', 'y', 'true', '1') THEN TRUE
                WHEN LOWER(TRIM(insurance_claimed)) IN ('no', 'n', 'false', '0') THEN FALSE
                ELSE NULL
            END AS insurance_claimed,

            CONCAT("TECH", CAST(technician_id AS STRING)) AS technician_id,
            
            {{ parse_full_name('technician_name') }}.prefix AS technician_prefix,
            {{ parse_full_name('technician_name') }}.first_name AS technician_first_name,
            {{ parse_full_name('technician_name') }}.surname AS technician_surname,

            {{ standardise_uk_mobiles('technician_mobile') }} AS technician_mobile,

            CONCAT("CUST", CAST(customer_id AS STRING)) AS customer_id,

            vehicle_age_in_years, 

            CASE
                WHEN LOWER(TRIM(weather_condition)) IN ('n/a', '', 'na') THEN NULL
                WHEN LOWER(TRIM(weather_condition)) IN ('clear') THEN 'Clear'
                WHEN LOWER(TRIM(weather_condition)) IN ('rain', 'rainy') THEN 'Rain'
                WHEN LOWER(TRIM(weather_condition)) IN ('fog', 'foggy') THEN 'Fog'
                WHEN LOWER(TRIM(weather_condition)) IN ('snow', 'snowy') THEN 'Snow'
                WHEN LOWER(TRIM(weather_condition)) IN ('windy', 'wind') THEN 'Windy'
                ELSE INITCAP(TRIM(weather_condition))
            END AS weather_condition,

            CASE
                WHEN TRIM(LOWER(traffic_level)) = 'low' THEN 'Low'
                WHEN TRIM(LOWER(traffic_level)) = 'medium' THEN 'Medium'
                WHEN TRIM(LOWER(traffic_level)) = 'high' THEN 'High'
                WHEN TRIM(LOWER(traffic_level)) = 'heavy' THEN 'Heavy'
                ELSE NULL
            END AS traffic_level,

            job_duration_in_hours 

        FROM source
        LEFT JOIN region_lookup rl
            ON LOWER(TRIM(source.city)) = rl.city  -- Added TRIM here too
    )
)

SELECT * FROM cleaned