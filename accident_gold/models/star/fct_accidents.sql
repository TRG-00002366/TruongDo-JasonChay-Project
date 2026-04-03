{{ config(materialized='table', transient=false) }}

SELECT
    id,

    -- foreign keys
    {{ dbt_utils.generate_surrogate_key([
        'street','city','county','state','zipcode','country',
        'start_lat','start_lng','end_lat','end_lng','timezone'
    ]) }} AS location_id,
    {{ dbt_utils.generate_surrogate_key(['start_time']) }} AS time_id,
    {{ dbt_utils.generate_surrogate_key([
        'weather_timestamp','weather_condition','wind_direction'
    ]) }} AS weather_id,

    -- measures
    severity,
    distance_mi,
    temperature_f,
    wind_chill_f,
    humidity,
    pressure_in,
    visibility_mi,
    wind_speed_mph,
    precipitation_in,

    -- attributes
    source,
    airport_code,

    -- flags
    amenity,
    bump,
    crossing,
    give_way,
    junction,
    no_exit,
    railway,
    roundabout,
    station,
    stop,
    traffic_calming,
    traffic_signal,
    turning_loop,

    -- degenerate
    description

FROM {{ source('clean', 'cleaned_accidents') }}
