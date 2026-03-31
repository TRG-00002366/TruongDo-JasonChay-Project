{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key([
        'weather_timestamp','weather_condition','wind_direction'
    ]) }} AS weather_id,

    weather_timestamp,
    weather_condition,
    wind_direction

FROM {{ source('clean', 'cleaned_accidents') }}