{{
    config(
        materialized='table',
        post_hook="COPY {{ this }} TO '../data/processed/marts/time_series/v1/fct_trips_monthly_v1.csv' (HEADER, DELIMITER ',')"
    )
}}

select
    -- Primary key
    month_start_date,

-- Time features (non-leaky)
year, month_num, month_name, quarter, days_in_month,

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

-- Trip volume lags (non-leaky)
trips_lag_1m,
trips_lag_3m,
trips_lag_12m,
trips_rolling_3m_avg,
trips_rolling_6m_avg,
trips_rolling_12m_avg,

-- Change indicators (non-leaky)
trips_change_1m_pct, trips_change_12m_pct,

-- User mix lags (non-leaky)
annual_member_ratio_lag_1m, annual_member_ratio_rolling_3m_avg,

-- Duration lags (non-leaky)

avg_trip_duration_lag_1m,
    avg_trip_duration_rolling_3m_avg

from {{ ref('int_combined_trip_monthly') }}