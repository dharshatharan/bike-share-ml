with merged_weather as (
    select
        coalesce(secondary_data.total_precip_mm, 0)::double as total_precip_mm,
        coalesce(secondary_data.snow_on_grnd_cm, 0)::bigint as snow_on_grnd_cm,
        primary_data.date_time,

        primary_data.year, -- fill missing total precipitation with 0, unsure if this is the best strategy as data is actually missing. 0.68% of the data is missing. Which is small enough.
        -- fill missing snow on ground with 0
        primary_data.month,

        primary_data.day,

        coalesce(primary_data.max_temp_c, secondary_data.max_temp_c)
            as max_temp_c,

        coalesce(primary_data.min_temp_c, secondary_data.min_temp_c)
            as min_temp_c,

        coalesce(primary_data.mean_temp_c, secondary_data.mean_temp_c)
            as mean_temp_c,

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
            when
                primary_data.mean_temp_c is null
                then secondary_data.station_name
            else primary_data.station_name
        end as station_name
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
        total_precip_mm,
        snow_on_grnd_cm,
        coalesce(
            coalesce(max_temp_c, (
                (
                    lag(max_temp_c) over (order by date_time)
                    + lead(max_temp_c) over (order by date_time)
                ) / 2.0
            )),
            max_temp_c
        ) as max_temp_c,
        coalesce(
            coalesce(min_temp_c, (
                (
                    lag(min_temp_c) over (order by date_time)
                    + lead(min_temp_c) over (order by date_time)
                ) / 2.0
            )),
            min_temp_c
        ) as min_temp_c,
        coalesce(
            coalesce(mean_temp_c, (
                (
                    lag(mean_temp_c) over (order by date_time)
                    + lead(mean_temp_c) over (order by date_time)
                ) / 2.0
            )),
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
    mean_temp_c,
    total_precip_mm,
    snow_on_grnd_cm
from filled_weather
