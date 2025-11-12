WITH source AS (
    SELECT * 
    FROM {{ source('raw_repairs_data', 'raw_repairs_data') }}
),

-- Lookup table for missing regions
region_lookup AS (
    SELECT
        LOWER(TRIM(city)) AS city,
        ANY_VALUE(TRIM(region)) AS region  
    FROM source
    WHERE TRIM(region) IS NOT NULL
      AND TRIM(region) != 'N/A'
      AND TRIM(region) != ''
      AND TRIM(city) IS NOT NULL
      AND TRIM(city) != 'N/A'
      AND TRIM(city) != ''
    GROUP BY LOWER(TRIM(city))  
),


-- Vehicle type mapping from seeds
vehicle_map AS (
    SELECT *
    FROM {{ ref('vehicle_type_mapping') }}
),

-- Garage type mapping from seeds
garage_type_map AS (
    SELECT *
    FROM {{ ref('garage_type_mapping') }}
),

-- Glass type mapping from seeds
glass_type_map AS (
    SELECT *
    FROM {{ ref('glass_type_mapping') }}
),

-- Damage type mapping from seeds
damage_type_map AS (
    SELECT *
    FROM {{ ref('damage_type_mapping') }}
),

-- Repair type mapping from seeds
repair_type_map AS (
    SELECT *
    FROM {{ ref('repair_type_mapping') }}
),

-- Window position mapping from seeds
window_position_map AS (
    SELECT *
    FROM {{ ref('window_position_mapping') }}
),

-- Weather Condition mapping
weather_condition_map AS (
    SELECT *
    FROM {{ ref('weather_condition_mapping') }}
),

-- Traffic level mapping
traffic_level_map AS (
    SELECT *
    FROM {{ ref('traffic_level_mapping') }}
),


cleaned AS (
    SELECT DISTINCT * 
    FROM (
        SELECT
            -- Standardise job ID
            CONCAT("JOB", CAST(job_ID AS STRING)) AS Job_ID,

            -- Parse repair dates (handles multiple formats)
            COALESCE(
                SAFE.PARSE_DATE('%d-%b-%y', repair_date),
                CASE
                    WHEN SAFE_CAST(SPLIT(repair_date, '/')[1] AS INT64) > 12
                        THEN SAFE.PARSE_DATE('%m/%d/%Y', repair_date)
                    ELSE SAFE.PARSE_DATE('%d/%m/%Y', repair_date)
                END
            ) AS repair_date,

            -- Standardise region using lookup table
            TRIM(
                INITCAP(
                    COALESCE(
                        NULLIF(TRIM(source.region), 'N/A'),
                        TRIM(rl.region) 
                    )
                )
            ) AS region,

            -- Clean city 
            TRIM(INITCAP(NULLIF(TRIM(source.city), 'N/A'))) AS city,

            -- Clean garage name
            TRIM(garage_name) AS garage_name, 

            -- Standardise garage type
            gatm.clean_value AS garage_type,

            -- Standardise vehicle brand, type, and model
            CASE
                WHEN UPPER(TRIM(Vehicle_Brand)) = 'BMW' THEN 'BMW'
                WHEN UPPER(TRIM(Vehicle_Brand)) = 'N/A' THEN NULL
                ELSE INITCAP(TRIM(Vehicle_Brand))
            END AS vehicle_brand,

            -- Use seed table for vehicle_type
            vm.clean_value AS vehicle_type,


            CASE
                WHEN UPPER(TRIM(Vehicle_Model)) = 'N/A' THEN NULL
                WHEN UPPER(TRIM(Vehicle_Model)) = 'R 1250 GS' THEN 'R 1250 GS'
                ELSE INITCAP(TRIM(Vehicle_Model))
            END AS vehicle_model,

            -- Standardise glass type
            gltm.clean_value AS glass_type,

            -- Standardise window position
            wpm.clean_value AS window_position,

            -- Standardise damage and repair type
            dtm.clean_value AS damage_type,

            rtm.clean_value AS repair_type,

            -- Clean and round costs
            {{ clean_currency('repair_cost') }} AS repair_cost,
            {{ clean_currency('glass_cost') }} AS glass_cost,
            ROUND({{ clean_currency('repair_cost') }} - {{ clean_currency('glass_cost') }}, 2) AS profit,
        
            -- Standardise customer rating
            CAST(
                CASE
                    WHEN LOWER(TRIM(customer_rating)) IN ('n/a', '') THEN NULL
                    WHEN LOWER(TRIM(customer_rating)) = 'one' THEN 1
                    WHEN LOWER(TRIM(customer_rating)) = 'five' THEN 5
                    ELSE SAFE_CAST(customer_rating AS INT64) 
                END AS INT64
            ) AS customer_rating,
            
            -- Parse customer names
            {{ parse_full_name('customer_name') }}.prefix AS customer_prefix,
            {{ parse_full_name('customer_name') }}.first_name AS customer_first_name,
            {{ parse_full_name('customer_name') }}.surname AS customer_surname,
            
            -- Clean and validate emails
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

            -- Standardise UK mobiles
            {{ standardise_uk_mobiles('customer_mobile') }} AS customer_mobile,
            
            -- Standardize UK postcodes
            -- - Null out invalid markers ('N/A', empty string)
            -- - Uppercase all characters
            -- - Reduce multiple spaces to single space
            CASE
                WHEN UPPER(TRIM(customer_postcode)) IN ('N/A', '') THEN NULL
                ELSE REGEXP_REPLACE(UPPER(TRIM(customer_postcode)), r'\s+', ' ')
            END AS customer_postcode,

            -- Boolean conversion of insurance claimed
            CASE
                WHEN LOWER(TRIM(insurance_claimed)) IN ('yes', 'y', 'true', '1') THEN TRUE
                WHEN LOWER(TRIM(insurance_claimed)) IN ('no', 'n', 'false', '0') THEN FALSE
                ELSE NULL
            END AS insurance_claimed,

            -- Standardise technician ID
            CONCAT("TECH", CAST(technician_id AS STRING)) AS technician_id,

            -- Parse technician names
            {{ parse_full_name('technician_name') }}.prefix AS technician_prefix,
            {{ parse_full_name('technician_name') }}.first_name AS technician_first_name,
            {{ parse_full_name('technician_name') }}.surname AS technician_surname,

            {{ standardise_uk_mobiles('technician_mobile') }} AS technician_mobile,

            -- Standardise customer ID
            CONCAT("CUST", CAST(customer_id AS STRING)) AS customer_id,

            vehicle_age_in_years, 

            -- Standardize weather conditions and traffic level
            wcm.clean_value AS weather_condition,

            tlm.clean_value AS traffic_level,

            job_duration_in_hours 

        FROM {{ source('raw_repairs_data', 'raw_repairs_data') }} AS source 
        LEFT JOIN region_lookup rl
            ON LOWER(TRIM(source.city)) = rl.city 
        LEFT JOIN vehicle_map vm
            ON UPPER(TRIM(source.vehicle_type)) = UPPER(TRIM(vm.raw_value))
        LEFT JOIN garage_type_map gatm
            ON UPPER(TRIM(source.garage_type)) = UPPER(TRIM(gatm.raw_value))
        LEFT JOIN glass_type_map gltm
            ON UPPER(TRIM(source.glass_type)) = UPPER(TRIM(gltm.raw_value))
        LEFT JOIN damage_type_map dtm
            ON UPPER(TRIM(source.damage_type)) = UPPER(TRIM(dtm.raw_value))
        LEFT JOIN repair_type_map rtm
            ON UPPER(TRIM(source.repair_type)) = UPPER(TRIM(rtm.raw_value))
        LEFT JOIN window_position_map wpm
            ON UPPER(TRIM(source.window_position)) = UPPER(TRIM(wpm.raw_value))
        LEFT JOIN weather_condition_map wcm
            ON UPPER(TRIM(source.weather_condition)) = UPPER(TRIM(wcm.raw_value))
        LEFT JOIN traffic_level_map tlm
            ON UPPER(TRIM(source.traffic_level)) = UPPER(TRIM(tlm.raw_value))
    )
)

SELECT * FROM cleaned