{{
    config(
        materialized='table',
        post_hook="COPY {{ this }} TO '../data/processed/marts/time_series/v1/fct_trips_daily_v1.csv' (HEADER, DELIMITER ',')"
    )
}}

select
    -- Primary key
    trip_date,

    -- Date & Time features (non-leaky)
    day_of_week,
    day_name,
    is_weekend,
    month_num,
    year,
    day_of_month,
    week_of_year,

    -- Trip volume features (LEAKY)
    total_trips__leaky,
    annual_member_trips__leaky,
    casual_member_trips__leaky,
    annual_member_ratio__leaky,
    unique_bikes__leaky,
    unique_start_stations__leaky,
    unique_end_stations__leaky,

    -- Trip duration features (LEAKY)
    avg_trip_duration__leaky,
    median_trip_duration__leaky,
    min_trip_duration__leaky,
    max_trip_duration__leaky,
    stddev_trip_duration__leaky,
    p25_trip_duration__leaky,
    p75_trip_duration__leaky,
    avg_annual_member_duration__leaky,
    avg_casual_member_duration__leaky,

    -- Weather features (non-leaky)
    max_temp_c,
    min_temp_c,
    mean_temp_c,
    temp_range_c,

    -- Station activity features (LEAKY)
    top_start_station_id__leaky,
    top_start_station_trips__leaky,
    top_end_station_id__leaky,
    top_end_station_trips__leaky,

    -- Trip volume lags (non-leaky)
    trips_lag_1d,
    trips_lag_2d,
    trips_lag_3d,
    trips_lag_7d,
    trips_lag_30d,
    trips_rolling_7d_avg,
    trips_rolling_7d_std,
    trips_rolling_30d_avg,
    trips_rolling_30d_std,

    -- User mix lags (non-leaky)
    annual_member_ratio_lag_1d,
    annual_member_ratio_lag_7d,
    annual_member_ratio_rolling_7d_avg,
    annual_member_trips_lag_7d,
    casual_member_trips_lag_7d,

    -- Duration lags (non-leaky)
    avg_trip_duration_lag_1d,
    avg_trip_duration_lag_7d,
    avg_trip_duration_rolling_7d_avg,
    avg_trip_duration_rolling_30d_avg,

    -- Network activity lags (non-leaky)
    unique_bikes_lag_1d,
    unique_bikes_lag_7d,
    unique_start_stations_lag_1d,
    unique_start_stations_lag_7d,
    unique_start_stations_rolling_30d_avg,

    -- Change indicators (non-leaky)

    trips_change_1d_pct,
    trips_change_7d_pct,
    trips_change_30d_pct

from {{ ref('int_combined_trip_daily') }}
