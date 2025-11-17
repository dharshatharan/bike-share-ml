{{
    config(
        materialized='table',
        post_hook="COPY {{ this }} TO '../data/processed/marts/time_series/v1/fct_trips_weekly_v1.csv' (HEADER, DELIMITER ',')"
    )
}}

select
    -- Primary key
    week_start_date,

    -- Time features (non-leaky)
    year,
    week_of_year,
    month_num,
    days_in_week,

    -- Trip volume features (LEAKY)
    total_trips__leaky,
    avg_daily_trips__leaky,
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

    -- Weather features (non-leaky)
    avg_max_temp_c,
    avg_min_temp_c,
    avg_mean_temp_c,
    max_temp_c,
    min_temp_c,
    avg_total_precip_mm,
    avg_snow_on_grnd_cm,
    max_total_precip_mm,
    max_snow_on_grnd_cm,
    min_total_precip_mm,
    min_snow_on_grnd_cm,

    -- Trip volume lags (non-leaky)
    trips_lag_1w,
    trips_lag_2w,
    trips_lag_4w,
    trips_lag_12w,
    trips_lag_52w,

    -- Trip volume rolling statistics (non-leaky)
    trips_rolling_4w_avg,
    trips_rolling_4w_std,
    trips_rolling_4w_min,
    trips_rolling_4w_max,
    trips_rolling_4w_median,
    trips_rolling_12w_avg,
    trips_rolling_12w_std,
    trips_rolling_12w_min,
    trips_rolling_12w_max,
    trips_rolling_12w_median,
    trips_rolling_52w_avg,
    trips_rolling_52w_std,
    trips_rolling_52w_min,
    trips_rolling_52w_max,
    trips_rolling_52w_median,

    -- Change indicators (non-leaky)
    trips_change_1w_vs_2w_pct,
    trips_change_52w_vs_104w_pct,

    -- User mix lags (non-leaky)
    annual_member_ratio_lag_1w,
    annual_member_ratio_rolling_4w_avg,

    -- Duration lags (non-leaky)
    avg_trip_duration_lag_1w,
    avg_trip_duration_rolling_4w_avg

from {{ ref('int_combined_trip_weekly') }}
