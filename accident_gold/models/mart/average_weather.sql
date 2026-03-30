{{ config(materialized='table') }}

SELECT
    Severity,
    ROUND(AVG("Precipitation(in)"), 2) AS avg_precipitation_in,
    ROUND(AVG("Temperature(F)"), 2) AS avg_temperature_f,
    ROUND(AVG("Visibility(mi)"), 2) AS avg_visibility_mi
FROM {{ source('raw', 'cleaned_accidents') }}
WHERE Severity IS NOT NULL
GROUP BY Severity
ORDER BY Severity