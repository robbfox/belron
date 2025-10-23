WITH source AS (
    SELECT * 
    FROM {{ source('raw_repairs_data', 'raw_repairs') }}
),

-- Lookup table for missing regions
region_lookup AS (
    SELECT
        LOWER(city) AS city,
        ANY_VALUE(region) AS region
    FROM source
    WHERE region IS NOT NULL
      AND region != 'N/A'
      AND city IS NOT NULL
      AND city != 'N/A'
    GROUP BY LOWER(city)
),



cleaned AS (
    SELECT DISTINCT * 
    FROM (
        SELECT

            CONCAT("JOB", CAST(job_ID AS STRING)) AS Job_ID,

            COALESCE(
                SAFE.PARSE_DATE('%d-%b-%y', repair_date),  -- e.g., 21-Oct-25
                CASE
                    WHEN SAFE_CAST(SPLIT(repair_date, '/')[1] AS INT64) > 12
                        THEN SAFE.PARSE_DATE('%m/%d/%Y', repair_date)  -- US format
                    ELSE SAFE.PARSE_DATE('%d/%m/%Y', repair_date)       -- UK format
                END
            ) AS repair_date,

            INITCAP(
                COALESCE(
                    NULLIF(TRIM(source.region), 'N/A'),
                    rl.region
                )
            ) AS region,

            INITCAP(NULLIF(TRIM(source.city), 'N/A')) AS city,


            garage_name,


            CASE 
            -- First, we standardize the input by removing whitespace and making it uppercase 
            -- Then, we group all variations of "Franchise" and "Chain" together 
            WHEN UPPER(TRIM(Garage_Type)) IN ('FRANCHSIE', 'CHAIN') THEN 'Franchise'
            -- We do the same for "Independent" 
            WHEN UPPER(TRIM(Garage_Type)) = 'INDEPENDENT' THEN 'Independent' 
            -- And for "Mobile" 
            WHEN UPPER(TRIM(Garage_Type)) = 'MOBILE' THEN 'Mobile' 
            -- Any other value, including 'N/A' or a real NULL, will become a standard NULL 
            ELSE NULL 
            END AS garage_type,

            CASE
                -- First, handle acronyms that should be fully uppercase
                WHEN UPPER(TRIM(Vehicle_Brand)) = 'BMW' THEN 'BMW'
                        
                -- Next, handle the explicit 'N/A' values
                WHEN UPPER(TRIM(Vehicle_Brand)) = 'N/A' THEN NULL
                        
                -- For everything else, apply title case for consistency
                -- INITCAP('land rover') -> 'Land Rover'
                -- INITCAP('mercedes-benz') -> 'Mercedes-Benz'
                ELSE INITCAP(TRIM(Vehicle_Brand))
            END AS vehicle_brand,

            CASE
                WHEN UPPER(TRIM(Vehicle_Type)) = 'SUV' THEN 'SUV' -- Keep acronyms uppercase
                WHEN UPPER(TRIM(Vehicle_Type)) = 'CAR' THEN 'Car'
                WHEN UPPER(TRIM(Vehicle_Type)) = 'VAN' THEN 'Van'
                WHEN UPPER(TRIM(Vehicle_Type)) = 'TRUCK' THEN 'Truck'
                WHEN UPPER(TRIM(Vehicle_Type)) IN ('MOTORCYCLE', 'MOTORBIKE') THEN 'Motorcycle' -- Grouping synonyms
                ELSE NULL -- This handles 'N/A', actual NULLs, and any other unexpected values
            END AS vehicle_type,

            CASE
                -- Rule 1: Handle the explicit null marker first
                WHEN UPPER(TRIM(Vehicle_Model)) = 'N/A' THEN NULL
                
                -- Rule 2: Handle specific, tricky cases that INITCAP won't format correctly
                WHEN UPPER(TRIM(Vehicle_Model)) = 'R 1250 GS' THEN 'R 1250 GS'
                
                -- Rule 3 (The Default): For everything else, apply TRIM and INITCAP
                ELSE INITCAP(TRIM(Vehicle_Model))
            END AS vehicle_model,

            -- Standardize Glass_Type names
            CASE
                -- Rule 1: Handle the explicit null marker first
                WHEN UPPER(TRIM(Glass_Type)) = 'N/A' THEN NULL
            
                WHEN UPPER(TRIM(Glass_Type)) = 'OEM' THEN 'OEM'
                ELSE INITCAP(TRIM(Glass_Type))
            END AS glass_type,

            -- check from this point on:
            CASE
                WHEN UPPER(TRIM(REPLACE(Window_Position, '_', ' '))) IN ('WINDSHIELD', 'WIND SCREEN')
                    THEN 'Windscreen'
                WHEN UPPER(TRIM(Window_Position)) = 'N/A'
                    THEN NULL
                ELSE INITCAP(TRIM(Window_Position))
            END AS window_position,

            INITCAP(TRIM(Damage_Type)) AS damage_type,

            INITCAP(TRIM(Repair_Type)) AS repair_type,
        
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
                -- First, check if the email is already structurally valid.
                WHEN REGEXP_CONTAINS(LOWER(TRIM(customer_email)), r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
                    THEN LOWER(TRIM(customer_email))
    
                -- Next, fix common errors where '@' is missing before a known major provider.
                WHEN customer_email LIKE '%gmail.com' AND NOT customer_email LIKE '%@%'
                    THEN REGEXP_REPLACE(LOWER(TRIM(customer_email)), r'(gmail\.com)', r'@\1')
            
                WHEN customer_email LIKE '%yahoo.co.uk' AND NOT customer_email LIKE '%@%'
                    THEN REGEXP_REPLACE(LOWER(TRIM(customer_email)), r'(yahoo\.co\.uk)', r'@\1')
    
                -- NEWLY ADDED to handle yahoo.com
                WHEN customer_email LIKE '%yahoo.com' AND NOT customer_email LIKE '%@%'
                    THEN REGEXP_REPLACE(LOWER(TRIM(customer_email)), r'(yahoo\.com)', r'@\1')
    
                WHEN customer_email LIKE '%hotmail.com' AND NOT customer_email LIKE '%@%'
                    THEN REGEXP_REPLACE(LOWER(TRIM(customer_email)), r'(hotmail\.com)', r'@\1')
    
                -- NEWLY ADDED to handle hotmail.co.uk
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
                
            
                -- If none of the above conditions are met, the email is invalid.
                ELSE NULL
            END AS customer_email,

            {{ clean_mobile('customer_mobile') }} AS customer_mobile,
            
            CASE
                WHEN UPPER(TRIM(customer_postcode)) IN ('N/A', '') THEN NULL
                ELSE UPPER(TRIM(customer_postcode))
            END AS customer_postcode,

            CASE
                WHEN LOWER(insurance_claimed) IN ('yes', 'y', 'true', '1') THEN TRUE
                WHEN LOWER(insurance_claimed) IN ('no', 'n', 'false', '0') THEN FALSE
                ELSE NULL
            END AS insurance_claimed,

            CONCAT("TECH", CAST(technician_id AS STRING)) AS technician_id,
            
            {{ parse_full_name('technician_name') }}.prefix AS technician_prefix,
            {{ parse_full_name('technician_name') }}.first_name AS technician_first_name,
            {{ parse_full_name('technician_name') }}.surname AS technician_surname,

            {{ clean_mobile('technician_mobile') }} AS technician_mobile,

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

        FROM {{ source('raw_repairs_data', 'raw_repairs') }} AS source 
        LEFT JOIN region_lookup rl
            ON LOWER(source.city) = rl.city

        )
)

SELECT * FROM cleaned