{{ config(materialized='table') }}

SELECT
    s.id,

    -- foreign keys
    loc.location_id,
    t.time_id,
    w.weather_id,

    -- measures
    s.severity,
    s.distance_mi,
    s.temperature_f,
    s.wind_chill_f,
    s.humidity,
    s.pressure_in,
    s.visibility_mi,
    s.wind_speed_mph,
    s.precipitation_in,

    -- attributes
    s.source,
    s.airport_code,

    -- flags
    s.amenity,
    s.bump,
    s.crossing,
    s.give_way,
    s.junction,
    s.no_exit,
    s.railway,
    s.roundabout,
    s.station,
    s.stop,
    s.traffic_calming,
    s.traffic_signal,
    s.turning_loop,

    -- degenerate
    s.description

FROM {{ source('clean', 'cleaned_accidents') }} s

LEFT JOIN {{ ref('dim_location') }} loc
  ON s.city = loc.city
 AND s.state = loc.state
 AND s.street = loc.street

LEFT JOIN {{ ref('dim_time') }} t
  ON s.start_time = t.start_time

LEFT JOIN {{ ref('dim_weather') }} w
  ON s.weather_timestamp = w.weather_timestamp