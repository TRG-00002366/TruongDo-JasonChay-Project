{{ config(materialized='table') }}

SELECT
    Weather_Condition,
    COUNT(*) AS accident_count,
    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rank
FROM {{ source('raw', 'cleaned_accidents') }}
WHERE Weather_Condition IS NOT NULL
GROUP BY Weather_Condition
QUALIFY rank <= 10
ORDER BY rank