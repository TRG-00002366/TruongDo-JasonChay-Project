{{ config(materialized='table') }}

WITH state_counts AS (
    SELECT
        State,
        COUNT(*) AS accident_count
FROM {{ source('raw', 'cleaned_accidents') }}
    WHERE State IS NOT NULL
    GROUP BY State
)

SELECT
    State,
    accident_count,
    ROW_NUMBER() OVER (ORDER BY accident_count DESC) AS rank
FROM {{ source('raw', 'cleaned_accidents') }}
ORDER BY rank