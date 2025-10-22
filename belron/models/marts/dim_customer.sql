WITH stg_repairs AS (
    SELECT * FROM {{ ref('stg_autoglass_repairs') }}
)
SELECT
    -- Create a unique surrogate key for each distinct combination of the columns below.
    {{ dbt_utils.generate_surrogate_key(['customer_id', 'customer_prefix', 'customer_first_name', 'customer_surname', 'customer_email', 'customer_mobile', 'customer_postcode']) }} AS customer_key,
    
    customer_id,
    customer_prefix,
    customer_first_name,
    customer_surname,
    customer_email,
    customer_mobile,
    customer_postcode

FROM stg_repairs

    WHERE customer_id IS NOT NULL
      AND customer_email IS NOT NULL
      AND customer_mobile IS NOT NULL
      AND customer_postcode IS NOT NULL
GROUP BY
    
    2, 3, 4, 5, 6, 7, 8