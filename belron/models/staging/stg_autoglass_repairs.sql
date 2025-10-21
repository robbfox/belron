with source as (

    select * from {{ source('raw_repairs_data', 'raw_repairs') }}

),

cleaned as (

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
        glass_type,
        window_position,
        damage_type,
        repair_type,
        repair_cost,
        glass_cost,
        profit,
        customer_rating,
        customer_name,
        customer_email,
        customer_mobile,
        customer_postcode,
        insurance_claimed,
        CONCAT("TECH", CAST(technician_id AS STRING)) AS technician_id,
        technician_name,
        technician_mobile,
        CONCAT("CUST", CAST(job_ID AS STRING)) AS customer_id,
        vehicle_age_in_years,
        weather_condition,
        traffic_level,
        job_duration_in_hours

    from source

)

select * from cleaned