with source as (

    select * from {{ source('raw_repairs_data', 'raw_repairs') }}

),

cleaned as (

    SELECT DISTINCT * FROM (
    select
        CONCAT("JOB", CAST(job_ID AS STRING)) AS Job_ID,
        COALESCE(
            SAFE.PARSE_DATE('%d-%b-%y', repair_date),  -- e.g., 21-Oct-25
            CASE
                WHEN SAFE_CAST(SPLIT(repair_date, '/')[1] AS INT64) > 12
                    THEN SAFE.PARSE_DATE('%m/%d/%Y', repair_date)  -- US format
                ELSE SAFE.PARSE_DATE('%d/%m/%Y', repair_date)       -- UK format
            END
        ) AS repair_date,
        region,
        city,
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
        -- models/staging/stg_repairs.sql

        -- Standardize Glass_Type names
        CASE
            -- Rule 1: Handle the explicit null marker first
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
        INITCAP(TRIM(Damage_Type)) AS damage_type,
        INITCAP(TRIM(Repair_Type)) AS repair_type,
       {{ clean_currency('repair_cost') }} AS repair_cost,
       {{ clean_currency('glass_cost') }} AS glass_cost,
        profit,
        customer_rating,
        {{ parse_full_name('customer_name') }}.prefix AS customer_prefix,
        {{ parse_full_name('customer_name') }}.first_name AS customer_first_name,
        {{ parse_full_name('customer_name') }}.surname AS customer_surname,
        -- models/staging/stg_autoglass_repairs.sql

        -- Clean and validate the customer_email column
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
        {{ standardize_uk_mobile('customer_mobile') }} AS customer_mobile,
        customer_postcode,
        CASE
        WHEN LOWER(insurance_claimed) IN ('yes', 'y', 'true', '1') THEN TRUE
        WHEN LOWER(insurance_claimed) IN ('no', 'n', 'false', '0') THEN FALSE
        ELSE NULL
        END AS insurance_claimed,
        CONCAT("TECH", CAST(technician_id AS STRING)) AS technician_id,
        {{ parse_full_name('technician_name') }}.prefix AS technician_prefix,
        {{ parse_full_name('technician_name') }}.first_name AS technician_first_name,
        {{ parse_full_name('technician_name') }}.surname AS technician_surname,
        {{ standardize_uk_mobile('technician_mobile') }} AS technician_mobile,
        CONCAT("CUST", CAST(customer_id AS STRING)) AS customer_id,
        vehicle_age_in_years,
        weather_condition,
        traffic_level,
        job_duration_in_hours

    from source

)
)

select * from cleaned