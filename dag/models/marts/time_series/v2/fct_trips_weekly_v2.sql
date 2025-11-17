{{
    config(
        materialized='table',
        post_hook="COPY {{ this }} TO '../data/processed/marts/time_series/v2/fct_trips_weekly_v2.csv' (HEADER, DELIMITER ',')"
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

    -- Target variables (LEAKY - what we're predicting)
    total_trips__leaky,
    avg_trip_duration__leaky,

    -- Weather features (non-leaky)
    avg_mean_temp_c,
    avg_total_precip_mm,

    -- Trip volume features (non-leaky - predictive)
    trips_lag_52w,
    trips_rolling_4w_avg

from {{ ref('int_combined_trip_weekly') }}
