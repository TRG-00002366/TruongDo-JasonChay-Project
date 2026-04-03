{{ config(materialized='table', transient=false) }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key([
        'street','city','county','state','zipcode','country',
        'start_lat','start_lng','end_lat','end_lng','timezone'
    ]) }} AS location_id,

    street,
    city,
    county,
    state,
    zipcode,
    country,
    start_lat,
    start_lng,
    end_lat,
    end_lng,
    timezone

FROM {{ source('clean', 'cleaned_accidents') }}