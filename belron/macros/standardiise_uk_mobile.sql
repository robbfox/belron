-- macros/standardize_uk_mobile.sql

{% macro standardize_uk_mobile(column_name) %}

    -- This macro takes a column containing UK mobile numbers
    -- and standardizes them to the international +44 format.
    -- It handles numbers starting with '07' or '447' and removes non-numeric characters.
    
    CASE
        -- First, we create a cleaned version of the number with only digits.
        {% set cleaned_number = "REGEXP_REPLACE(" ~ column_name ~ ", r'[^0-9]', '')" %}

        -- Rule 1: Check if the cleaned number starts with '07' and has 11 digits.
        WHEN REGEXP_CONTAINS({{ cleaned_number }}, r'^07\d{9}$')
            -- If it matches, replace the leading '0' with '+44'.
            THEN CONCAT('+44', SUBSTR({{ cleaned_number }}, 2))
            
        -- Rule 2: Check if the number already starts with '447' and has 12 digits.
        WHEN REGEXP_CONTAINS({{ cleaned_number }}, r'^447\d{9}$')
            -- If it matches, just add the '+' at the beginning.
            THEN CONCAT('+', {{ cleaned_number }})
            
        -- If the number is not a valid UK mobile format, return NULL.
        ELSE NULL
    END

{% endmacro %}