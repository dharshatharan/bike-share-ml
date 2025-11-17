{{
    config(
        materialized='table',
        post_hook="COPY {{ this }} TO '../data/processed/marts/time_series/v2/fct_trips_daily_v2.csv' (HEADER, DELIMITER ',')"
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

    -- Target variables (LEAKY - what we're predicting)
    total_trips__leaky,
    avg_trip_duration__leaky,

    -- Weather features (non-leaky)
    mean_temp_c,
    total_precip_mm,

    -- Trip volume features (non-leaky - predictive)
    trips_lag_7d,
    trips_rolling_7d_avg,

    -- User mix features (non-leaky - predictive)
    annual_member_ratio_lag_7d

from {{ ref('int_combined_trip_daily') }}
