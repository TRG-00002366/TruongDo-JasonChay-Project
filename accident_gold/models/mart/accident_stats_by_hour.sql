
{{ config(materialized='table') }}

SELECT
    EXTRACT(HOUR FROM TO_TIMESTAMP(Start_Time)) AS hour_of_day,
    COUNT(*) AS total_accidents,
    ROUND(AVG(Severity), 2) AS avg_severity
FROM {{ source('raw', 'clean_accidents') }}
GROUP BY hour_of_day
ORDER BY hour_of_day;