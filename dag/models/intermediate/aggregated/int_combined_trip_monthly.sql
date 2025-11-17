with monthly_base as (
    select
        date_trunc('month', trip_date) as month_start_date,

        -- Time features (non-leaky)
        year(
            date_trunc('month', trip_date)
        ) as year,
        month(
            date_trunc('month', trip_date)
        ) as month_num,
        monthname(
            date_trunc('month', trip_date)
        ) as month_name,
        quarter(
            date_trunc('month', trip_date)
        ) as quarter,
        count(distinct trip_date) as days_in_month,

        -- Holiday and event features (non-leaky)
        sum(case when is_weekend or is_holiday then 1 else 0 end)::double
        / count(distinct trip_date)::double as non_working_day_ratio,
        bool_or(has_covid_restrictions) as has_covid_restrictions,

        -- Trip volume features (LEAKY)
        sum(total_trips__leaky) as total_trips__leaky,
        avg(total_trips__leaky) as avg_daily_trips__leaky,
        sum(annual_member_trips__leaky) as annual_member_trips__leaky,
        sum(casual_member_trips__leaky) as casual_member_trips__leaky,
        sum(annual_member_trips__leaky)::double
        / nullif(sum(total_trips__leaky), 0)::double
            as annual_member_ratio__leaky,
        avg(unique_bikes__leaky) as unique_bikes__leaky,
        avg(unique_start_stations__leaky) as unique_start_stations__leaky,
        avg(unique_end_stations__leaky) as unique_end_stations__leaky,

        -- Trip duration features (LEAKY)
        avg(avg_trip_duration__leaky) as avg_trip_duration__leaky,
        avg(median_trip_duration__leaky) as median_trip_duration__leaky,
        min(min_trip_duration__leaky) as min_trip_duration__leaky,
        max(max_trip_duration__leaky) as max_trip_duration__leaky,

        -- Weather features (non-leaky)

        avg(max_temp_c) as avg_max_temp_c,
        avg(min_temp_c) as avg_min_temp_c,
        avg(mean_temp_c) as avg_mean_temp_c,
        max(max_temp_c) as max_temp_c,
        min(min_temp_c) as min_temp_c,
        avg(total_precip_mm) as avg_total_precip_mm,
        avg(snow_on_grnd_cm) as avg_snow_on_grnd_cm,
        max(total_precip_mm) as max_total_precip_mm,
        max(snow_on_grnd_cm) as max_snow_on_grnd_cm,
        min(total_precip_mm) as min_total_precip_mm,
        min(snow_on_grnd_cm) as min_snow_on_grnd_cm
    from {{ ref('int_combined_trip_daily') }}
    group by date_trunc('month', trip_date)
),

lagged_features as (
    select
        *,

        -- Trip volume lags (non-leaky)
        lag(total_trips__leaky, 1) over (
            order by month_start_date
        ) as trips_lag_1m,
        lag(total_trips__leaky, 2) over (
            order by month_start_date
        ) as trips_lag_2m,
        lag(total_trips__leaky, 3) over (
            order by month_start_date
        ) as trips_lag_3m,
        lag(total_trips__leaky, 6) over (
            order by month_start_date
        ) as trips_lag_6m,
        lag(total_trips__leaky, 12) over (
            order by month_start_date
        ) as trips_lag_12m,

        -- Rolling trip statistics - 3 month (non-leaky, excludes current month)
        avg(total_trips__leaky) over (
            order by month_start_date rows between 3 preceding
            and 1 preceding
        ) as trips_rolling_3m_avg,
        stddev(total_trips__leaky) over (
            order by month_start_date rows between 3 preceding
            and 1 preceding
        ) as trips_rolling_3m_std,
        min(total_trips__leaky) over (
            order by month_start_date rows between 3 preceding
            and 1 preceding
        ) as trips_rolling_3m_min,
        max(total_trips__leaky) over (
            order by month_start_date rows between 3 preceding
            and 1 preceding
        ) as trips_rolling_3m_max,
        median(total_trips__leaky) over (
            order by month_start_date rows between 3 preceding
            and 1 preceding
        ) as trips_rolling_3m_median,

        -- Rolling trip statistics - 6 month (non-leaky, excludes current month)
        avg(total_trips__leaky) over (
            order by month_start_date rows between 6 preceding
            and 1 preceding
        ) as trips_rolling_6m_avg,
        stddev(total_trips__leaky) over (
            order by month_start_date rows between 6 preceding
            and 1 preceding
        ) as trips_rolling_6m_std,
        min(total_trips__leaky) over (
            order by month_start_date rows between 6 preceding
            and 1 preceding
        ) as trips_rolling_6m_min,
        max(total_trips__leaky) over (
            order by month_start_date rows between 6 preceding
            and 1 preceding
        ) as trips_rolling_6m_max,
        median(total_trips__leaky) over (
            order by month_start_date rows between 6 preceding
            and 1 preceding
        ) as trips_rolling_6m_median,

        -- Rolling trip statistics - 12 month (non-leaky)
        avg(total_trips__leaky) over (
            order by month_start_date rows between 12 preceding
            and 1 preceding
        ) as trips_rolling_12m_avg,
        stddev(total_trips__leaky) over (
            order by month_start_date rows between 12 preceding
            and 1 preceding
        ) as trips_rolling_12m_std,
        min(total_trips__leaky) over (
            order by month_start_date rows between 12 preceding
            and 1 preceding
        ) as trips_rolling_12m_min,
        max(total_trips__leaky) over (
            order by month_start_date rows between 12 preceding
            and 1 preceding
        ) as trips_rolling_12m_max,
        median(total_trips__leaky) over (
            order by month_start_date rows between 12 preceding
            and 1 preceding
        ) as trips_rolling_12m_median,

        -- Change indicators (non-leaky - comparing historical periods)
        case
            when
                lag(total_trips__leaky, 2)
                    over (order by month_start_date)
                > 0
                then (
                    (
                        lag(total_trips__leaky, 1)
                            over (order by month_start_date)
                        - lag(total_trips__leaky, 2)
                            over (order by month_start_date)
                    )::double
                    / lag(total_trips__leaky, 2)
                        over (order by month_start_date)
                    ::double
                ) * 100
        end as trips_change_1m_vs_2m_pct,

        case
            when
                lag(total_trips__leaky, 24)
                    over (order by month_start_date)
                > 0
                then (
                    (
                        lag(total_trips__leaky, 12)
                            over (order by month_start_date)
                        - lag(total_trips__leaky, 24)
                            over (order by month_start_date)
                    )::double
                    / lag(total_trips__leaky, 24)
                        over (order by month_start_date)
                    ::double
                ) * 100
        end as trips_change_12m_vs_24m_pct,

        -- User mix lags (non-leaky)
        lag(annual_member_ratio__leaky, 1) over (
            order by month_start_date
        ) as annual_member_ratio_lag_1m,
        avg(annual_member_ratio__leaky) over (
            order by month_start_date rows between 3 preceding
            and 1 preceding
        ) as annual_member_ratio_rolling_3m_avg,

        -- Duration lags (non-leaky)
        lag(avg_trip_duration__leaky, 1)
            over (order by month_start_date)
            as avg_trip_duration_lag_1m,
        avg(avg_trip_duration__leaky)
            over (
                order by
                    month_start_date
                rows between 3 preceding and 1 preceding
            )
            as avg_trip_duration_rolling_3m_avg

    from monthly_base
)

select * from lagged_features
order by month_start_date
