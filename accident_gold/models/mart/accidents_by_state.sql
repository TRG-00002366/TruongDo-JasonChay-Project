{{ config(materialized='table') }}

SELECT
    state,
    COUNT(*) AS total_accidents,
FROM {{ source('raw', 'cleaned_accidents') }}
    WHERE state IS NOT NULL
    GROUP BY state
    ORDER BY total_accidents DESC