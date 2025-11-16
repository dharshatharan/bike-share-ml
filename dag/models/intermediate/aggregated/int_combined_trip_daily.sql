with daily_base as (
    select
        start_time::date as trip_date,

        -- Date & Time features (non-leaky)
        dayofweek(start_time::date) as day_of_week,
        dayname(start_time::date) as day_name,
        coalesce(dayofweek(start_time::date) in (6, 7), false) as is_weekend,
        month(start_time::date) as month_num,
        year(start_time::date) as year,
        day(start_time::date) as day_of_month,
        weekofyear(start_time::date) as week_of_year,

        -- Trip volume features (LEAKY)
        count(*) as total_trips__leaky,
        count(*) filter (where user_type = 'Annual Member')
            as annual_member_trips__leaky,
        count(*) filter (where user_type = 'Casual Member')
            as casual_member_trips__leaky,
        count(*) filter (where user_type = 'Annual Member')::double
        / nullif(count(*), 0)::double as annual_member_ratio__leaky,
        count(distinct bike_id) as unique_bikes__leaky,
        count(distinct start_station_id) as unique_start_stations__leaky,
        count(distinct end_station_id) as unique_end_stations__leaky,

        -- Trip duration features (LEAKY)
        avg(trip_duration) as avg_trip_duration__leaky,
        median(trip_duration) as median_trip_duration__leaky,
        min(trip_duration) as min_trip_duration__leaky,
        max(trip_duration) as max_trip_duration__leaky,
        stddev(trip_duration) as stddev_trip_duration__leaky,
        percentile_cont(0.25) within group (
            order by trip_duration
        ) as p25_trip_duration__leaky,
        percentile_cont(0.75) within group (
            order by trip_duration
        ) as p75_trip_duration__leaky,
        avg(trip_duration) filter (
            where
            user_type = 'Annual Member'
        ) as avg_annual_member_duration__leaky,
        avg(trip_duration) filter (
            where
            user_type = 'Casual Member'
        ) as avg_casual_member_duration__leaky,

        -- Weather features (non-leaky - known at prediction time)

        max(weather_max_temp_c) as max_temp_c,
        max(weather_min_temp_c) as min_temp_c,
        max(weather_mean_temp_c) as mean_temp_c,
        max(weather_max_temp_c) - max(weather_min_temp_c) as temp_range_c,

        max(weather_total_precip_mm) as total_precip_mm,
        max(weather_snow_on_grnd_cm) as snow_on_grnd_cm

    from {{ ref('int_combined_trip_data') }}
    group by start_time::date
),

station_rankings as (
    select
        start_time::date as trip_date,
        start_station_id,
        end_station_id,
        count(*) as trip_count,
        row_number()
            over (partition by start_time::date order by count(*) desc)
            as start_station_rank,
        row_number()
            over (partition by start_time::date order by count(*) desc)
            as end_station_rank
    from {{ ref('int_combined_trip_data') }}
    group by start_time::date, start_station_id, end_station_id
),

top_start_stations as (
    select
        trip_date,
        start_station_id as top_start_station_id__leaky,
        trip_count as top_start_station_trips__leaky
    from station_rankings
    where start_station_rank = 1
),

top_end_stations as (
    select
        trip_date,
        end_station_id as top_end_station_id__leaky,
        trip_count as top_end_station_trips__leaky
    from station_rankings
    where end_station_rank = 1
),

daily_with_stations as (
    select
        daily_base.*,
        top_start.top_start_station_id__leaky,
        top_start.top_start_station_trips__leaky,
        top_end.top_end_station_id__leaky,
        top_end.top_end_station_trips__leaky
    from daily_base
    left join top_start_stations as top_start
        on daily_base.trip_date = top_start.trip_date
    left join top_end_stations as top_end
        on daily_base.trip_date = top_end.trip_date
),

lagged_features as (
    select
        *,

        -- Trip volume lags
        lag(total_trips__leaky, 1) over (
            order by trip_date
        ) as trips_lag_1d,
        lag(total_trips__leaky, 2) over (
            order by trip_date
        ) as trips_lag_2d,
        lag(total_trips__leaky, 3) over (
            order by trip_date
        ) as trips_lag_3d,
        lag(total_trips__leaky, 7) over (
            order by trip_date
        ) as trips_lag_7d,
        lag(total_trips__leaky, 30) over (
            order by trip_date
        ) as trips_lag_30d,

        -- Rolling trip statistics
        avg(total_trips__leaky) over (
            order by trip_date rows between 6 preceding
            and current row
        ) as trips_rolling_7d_avg,
        stddev(total_trips__leaky) over (
            order by trip_date rows between 6 preceding
            and current row
        ) as trips_rolling_7d_std,
        avg(total_trips__leaky) over (
            order by trip_date rows between 29 preceding
            and current row
        ) as trips_rolling_30d_avg,
        stddev(total_trips__leaky) over (
            order by trip_date rows between 29 preceding
            and current row
        ) as trips_rolling_30d_std,

        -- User mix lags
        lag(annual_member_ratio__leaky, 1) over (
            order by trip_date
        ) as annual_member_ratio_lag_1d,
        lag(annual_member_ratio__leaky, 7) over (
            order by trip_date
        ) as annual_member_ratio_lag_7d,
        avg(annual_member_ratio__leaky) over (
            order by trip_date rows between 6 preceding
            and current row
        ) as annual_member_ratio_rolling_7d_avg,
        lag(annual_member_trips__leaky, 7) over (
            order by trip_date
        ) as annual_member_trips_lag_7d,
        lag(casual_member_trips__leaky, 7) over (
            order by trip_date
        ) as casual_member_trips_lag_7d,

        -- Duration lags
        lag(avg_trip_duration__leaky, 1) over (
            order by trip_date
        ) as avg_trip_duration_lag_1d,
        lag(avg_trip_duration__leaky, 7) over (
            order by trip_date
        ) as avg_trip_duration_lag_7d,
        avg(avg_trip_duration__leaky) over (
            order by trip_date rows between 6 preceding
            and current row
        ) as avg_trip_duration_rolling_7d_avg,
        avg(avg_trip_duration__leaky) over (
            order by trip_date rows between 29 preceding
            and current row
        ) as avg_trip_duration_rolling_30d_avg,

        -- Network activity lags
        lag(unique_bikes__leaky, 1) over (
            order by trip_date
        ) as unique_bikes_lag_1d,
        lag(unique_bikes__leaky, 7) over (
            order by trip_date
        ) as unique_bikes_lag_7d,
        lag(
            unique_start_stations__leaky,
            1
        ) over (
            order by trip_date
        ) as unique_start_stations_lag_1d,
        lag(
            unique_start_stations__leaky,
            7
        ) over (
            order by trip_date
        ) as unique_start_stations_lag_7d,
        avg(unique_start_stations__leaky) over (
            order by trip_date rows between 29 preceding
            and current row
        ) as unique_start_stations_rolling_30d_avg,

        -- Change indicators

        case
            when lag(total_trips__leaky, 1) over (order by trip_date) > 0
                then (
                    (
                        total_trips__leaky
                        - lag(total_trips__leaky, 1) over (order by trip_date)
                    )::double
                    / lag(total_trips__leaky, 1)
                        over (order by trip_date)
                    ::double
                ) * 100
        end as trips_change_1d_pct,

        case
            when lag(total_trips__leaky, 7) over (order by trip_date) > 0
                then (
                    (
                        total_trips__leaky
                        - lag(total_trips__leaky, 7) over (order by trip_date)
                    )::double
                    / lag(total_trips__leaky, 7)
                        over (order by trip_date)
                    ::double
                ) * 100
        end as trips_change_7d_pct,

        case
            when lag(total_trips__leaky, 30) over (order by trip_date) > 0
                then (
                    (
                        total_trips__leaky
                        - lag(total_trips__leaky, 30) over (order by trip_date)
                    )::double
                    / lag(total_trips__leaky, 30)
                        over (order by trip_date)
                    ::double
                ) * 100
        end as trips_change_30d_pct

    from daily_with_stations
)

select * from lagged_features
order by trip_date
