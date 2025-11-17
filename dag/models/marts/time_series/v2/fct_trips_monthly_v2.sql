{{
    config(
        materialized='table',
        post_hook="COPY {{ this }} TO '../data/processed/marts/time_series/v2/fct_trips_monthly_v2.csv' (HEADER, DELIMITER ',')"
    )
}}

select
    -- Primary key
    month_start_date,

    -- Time features (non-leaky)
    year,
    month_num,
    month_name,
    quarter,
    days_in_month,
    non_working_day_ratio,
    has_covid_restrictions,

    -- Target variables (LEAKY - what we're predicting)
    total_trips__leaky,
    avg_trip_duration__leaky,

    -- Weather features (non-leaky)
    avg_mean_temp_c,
    avg_total_precip_mm,

    -- Trip volume features (non-leaky - predictive)
    trips_lag_12m,
    trips_rolling_3m_avg

from {{ ref('int_combined_trip_monthly') }}
