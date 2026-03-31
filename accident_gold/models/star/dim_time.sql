{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['start_time']) }} AS time_id,

    start_time,
    end_time,

    EXTRACT(HOUR FROM start_time) AS hour,
    EXTRACT(DAY FROM start_time) AS day,
    EXTRACT(MONTH FROM start_time) AS month,
    EXTRACT(YEAR FROM start_time) AS year,
    DAYNAME(start_time) AS day_of_week,

    CASE WHEN DAYOFWEEK(start_time) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend,

    sunrise_sunset,
    civil_twilight,
    nautical_twilight,
    astronomical_twilight

FROM {{ source('clean', 'cleaned_accidents') }}