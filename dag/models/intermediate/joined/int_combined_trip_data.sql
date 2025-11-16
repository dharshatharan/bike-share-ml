select
    -- trip info
    trips.trip_id as trip_id,
    trips.trip_duration as trip_duration,
    trips.start_station_id as start_station_id,
    trips.start_time as start_time,
    trips.end_station_id as end_station_id,
    trips.end_time as end_time,
    trips.bike_id as bike_id,
    trips.user_type as user_type,

-- start station info
start_station_info.name as start_station_name,
start_station_info.physical_configuration as start_station_physical_configuration,
start_station_info.lat as start_station_lat,
start_station_info.lon as start_station_lon,
start_station_info.address as start_station_address,
start_station_info.capacity as start_station_capacity,
start_station_info.is_charging_station as start_station_is_charging_station,
start_station_info.rental_methods as start_station_rental_methods,
start_station_info.groups as start_station_groups,
start_station_info.obcn as start_station_obcn,
start_station_info.short_name as start_station_short_name,
start_station_info.nearby_distance as start_station_nearby_distance,

-- end station info
end_station_info.name as end_station_name,
end_station_info.physical_configuration as end_station_physical_configuration,
end_station_info.lat as end_station_lat,
end_station_info.lon as end_station_lon,
end_station_info.address as end_station_address,
end_station_info.capacity as end_station_capacity,
end_station_info.is_charging_station as end_station_is_charging_station,
end_station_info.rental_methods as end_station_rental_methods,
end_station_info.groups as end_station_groups,
end_station_info.obcn as end_station_obcn,
end_station_info.short_name as end_station_short_name,
end_station_info.nearby_distance as end_station_nearby_distance,

-- weather info

weather.longitude as weather_longitude,
    weather.latitude as weather_latitude,
    weather.station_name as weather_station_name,
    weather.climate_id as weather_climate_id,
    weather.date_time as weather_date_time,
    weather.year as weather_year,
    weather.month as weather_month,
    weather.day as weather_day,
    weather.max_temp_c as weather_max_temp_c,
    weather.min_temp_c as weather_min_temp_c,
    weather.mean_temp_c as weather_mean_temp_c,
		weather.total_precip_mm as weather_total_precip_mm,
		weather.snow_on_grnd_cm as weather_snow_on_grnd_cm

from {{ ref('int_bike_share_trips_clean') }} as trips
left join {{ ref('int_bike_share_station_info_clean') }} as start_station_info
    on trips.start_station_id = start_station_info.station_id
left join {{ ref('int_bike_share_station_info_clean') }} as end_station_info
    on trips.end_station_id = end_station_info.station_id
left join {{ ref('int_weather_daily_clean') }} as weather
    on trips.start_time::date = weather.date_time::date