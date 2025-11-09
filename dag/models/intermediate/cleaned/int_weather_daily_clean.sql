
with merged_weather as (
    select
        coalesce(primary_data.max_temp_c, secondary_data.max_temp_c) as max_temp_c,
        coalesce(primary_data.min_temp_c, secondary_data.min_temp_c) as min_temp_c,
        coalesce(primary_data.mean_temp_c, secondary_data.mean_temp_c) as mean_temp_c,

        case
            when primary_data.mean_temp_c is null then secondary_data.climate_id
            else primary_data.climate_id
        end as climate_id,

        case
            when primary_data.mean_temp_c is null then secondary_data.latitude
            else primary_data.latitude
        end as latitude,

        case
            when primary_data.mean_temp_c is null then secondary_data.longitude
            else primary_data.longitude
        end as longitude,

        case
            when primary_data.mean_temp_c is null then secondary_data.station_name
            else primary_data.station_name
        end as station_name,

        primary_data.date_time,
        primary_data.year,
        primary_data.month,
        primary_data.day
    from {{ ref('stg_weather_daily_6158359') }} as primary_data
    left join {{ ref('stg_weather_daily_6158355') }} as secondary_data
        on primary_data.date_time = secondary_data.date_time
),
filled_weather as (
    select
        longitude,
        latitude,
        station_name,
        climate_id,
        date_time,
        year,
        month,
        day,
        -- fill missing days by averaging previous and next day values
        coalesce(
            case
                when max_temp_c is null then (
                    (lag(max_temp_c) over (order by date_time) +
                     lead(max_temp_c) over (order by date_time)
                    ) / 2.0
                )
                else max_temp_c
            end,
            max_temp_c
        ) as max_temp_c,
        coalesce(
            case
                when min_temp_c is null then (
                    (lag(min_temp_c) over (order by date_time) +
                     lead(min_temp_c) over (order by date_time)
                    ) / 2.0
                )
                else min_temp_c
            end,
            min_temp_c
        ) as min_temp_c,
        coalesce(
            case
                when mean_temp_c is null then (
                    (lag(mean_temp_c) over (order by date_time) +
                     lead(mean_temp_c) over (order by date_time)
                    ) / 2.0
                )
                else mean_temp_c
            end,
            mean_temp_c
        ) as mean_temp_c
    from merged_weather
)
select
    longitude,
    latitude,
    station_name,
    climate_id,
    date_time,
    year,
    month,
    day,
    max_temp_c,
    min_temp_c,
    mean_temp_c
from filled_weather