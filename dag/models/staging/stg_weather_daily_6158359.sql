select
    "Longitude (x)"::double as longitude,
    "Latitude (y)"::double as latitude,
    "Station Name"::varchar as station_name,
    "Climate ID"::bigint as climate_id,
    "Date/Time"::date as date_time,
    "Year"::bigint as year,
    "Month"::varchar as month,
    "Day"::varchar as day,
    "Data Quality"::varchar as data_quality,
    "Max Temp (°C)"::double as max_temp_c,
    "Max Temp Flag"::varchar as max_temp_flag,
    "Min Temp (°C)"::double as min_temp_c,
    "Min Temp Flag"::varchar as min_temp_flag,
    "Mean Temp (°C)"::double as mean_temp_c
from {{ source('weather', 'daily_weather_6158359') }}