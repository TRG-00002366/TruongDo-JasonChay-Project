{{ config(materialized='table') }}

SELECT
    severity,
    ROUND(AVG(Precipitation_in), 2) AS avg_precipitation,
    ROUND(AVG(Temperature_F), 2) AS avg_temperature,
    ROUND(AVG(Visibility_mi), 2) AS avg_visibility
FROM {{ source('clean', 'cleaned_accidents') }}
WHERE severity IS NOT NULL
GROUP BY severity
ORDER BY severity