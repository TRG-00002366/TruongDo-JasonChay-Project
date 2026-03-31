{{ config(materialized='table') }}

SELECT
    severity,
    ROUND(AVG("Precipitation(in)"), 2) AS avg_precipitation,
    ROUND(AVG("Temperature(F)"), 2) AS avg_temperature,
    ROUND(AVG("Visibility(mi)"), 2) AS avg_visibility
FROM {{ source('raw', 'cleaned_accidents') }}
WHERE severity IS NOT NULL
GROUP BY severity
ORDER BY severity