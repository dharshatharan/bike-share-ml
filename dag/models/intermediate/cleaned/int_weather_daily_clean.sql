select
    primary_data.longitude,
    primary_data.latitude,
    primary_data.station_name,
    case
        when primary_data.max_temp_c is null then secondary_data.climate_id
        else primary_data.climate_id
    end as climate_id,
    primary_data.date_time,
    primary_data.year,
    primary_data.month,
    primary_data.day,
    coalesce(primary_data.max_temp_c, secondary_data.max_temp_c) as max_temp_c,
    coalesce(primary_data.min_temp_c, secondary_data.min_temp_c) as min_temp_c,
    coalesce(primary_data.mean_temp_c, secondary_data.mean_temp_c) as mean_temp_c
from {{ ref('stg_weather_daily_6158359') }} as primary_data
join {{ ref('stg_weather_daily_6158355') }} as secondary_data
	on primary_data.date_time = secondary_data.date_time