{{ config(materialized='table') }}

SELECT
    weather_condition,
    COUNT(*) AS total_accidents,
FROM {{ source('clean', 'cleaned_accidents') }}
WHERE weather_condition IS NOT NULL
GROUP BY weather_condition
ORDER BY total_accidents DESC