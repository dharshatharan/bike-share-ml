with weekly_base as (
    select
        date_trunc('week', trip_date) as week_start_date,

        -- Time features (non-leaky)
        year(
            date_trunc('week', trip_date)
        ) as year,
        weekofyear(
            date_trunc('week', trip_date)
        ) as week_of_year,
        month(
            date_trunc('week', trip_date)
        ) as month_num,
        count(distinct trip_date) as days_in_week,

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
    group by date_trunc('week', trip_date)
),

lagged_features as (
    select
        *,

        -- Trip volume lags (non-leaky)
        lag(total_trips__leaky, 1) over (
            order by week_start_date
        ) as trips_lag_1w,
        lag(total_trips__leaky, 2) over (
            order by week_start_date
        ) as trips_lag_2w,
        lag(total_trips__leaky, 4) over (
            order by week_start_date
        ) as trips_lag_4w,
        lag(total_trips__leaky, 12) over (
            order by week_start_date
        ) as trips_lag_12w,
        lag(total_trips__leaky, 52) over (
            order by week_start_date
        ) as trips_lag_52w,

        -- Rolling trip statistics - 4 week (non-leaky, excludes current week)
        avg(total_trips__leaky) over (
            order by week_start_date rows between 4 preceding
            and 1 preceding
        ) as trips_rolling_4w_avg,
        stddev(total_trips__leaky) over (
            order by week_start_date rows between 4 preceding
            and 1 preceding
        ) as trips_rolling_4w_std,
        min(total_trips__leaky) over (
            order by week_start_date rows between 4 preceding
            and 1 preceding
        ) as trips_rolling_4w_min,
        max(total_trips__leaky) over (
            order by week_start_date rows between 4 preceding
            and 1 preceding
        ) as trips_rolling_4w_max,
        median(total_trips__leaky) over (
            order by week_start_date rows between 4 preceding
            and 1 preceding
        ) as trips_rolling_4w_median,

        -- Rolling trip statistics - 12 week (non-leaky, excludes current week)
        avg(total_trips__leaky) over (
            order by week_start_date rows between 12 preceding
            and 1 preceding
        ) as trips_rolling_12w_avg,
        stddev(total_trips__leaky) over (
            order by week_start_date rows between 12 preceding
            and 1 preceding
        ) as trips_rolling_12w_std,
        min(total_trips__leaky) over (
            order by week_start_date rows between 12 preceding
            and 1 preceding
        ) as trips_rolling_12w_min,
        max(total_trips__leaky) over (
            order by week_start_date rows between 12 preceding
            and 1 preceding
        ) as trips_rolling_12w_max,
        median(total_trips__leaky) over (
            order by week_start_date rows between 12 preceding
            and 1 preceding
        ) as trips_rolling_12w_median,

        -- Rolling trip statistics - 52 week (non-leaky, excludes current week)
        avg(total_trips__leaky) over (
            order by week_start_date rows between 52 preceding
            and 1 preceding
        ) as trips_rolling_52w_avg,
        stddev(total_trips__leaky) over (
            order by week_start_date rows between 52 preceding
            and 1 preceding
        ) as trips_rolling_52w_std,
        min(total_trips__leaky) over (
            order by week_start_date rows between 52 preceding
            and 1 preceding
        ) as trips_rolling_52w_min,
        max(total_trips__leaky) over (
            order by week_start_date rows between 52 preceding
            and 1 preceding
        ) as trips_rolling_52w_max,
        median(total_trips__leaky) over (
            order by week_start_date rows between 52 preceding
            and 1 preceding
        ) as trips_rolling_52w_median,

        -- Change indicators (non-leaky - comparing historical periods only)
        case
            when
                lag(total_trips__leaky, 2)
                    over (order by week_start_date)
                > 0
                then (
                    (
                        lag(total_trips__leaky, 1)
                            over (order by week_start_date)
                        - lag(total_trips__leaky, 2)
                            over (order by week_start_date)
                    )::double
                    / lag(total_trips__leaky, 2)
                        over (order by week_start_date)
                    ::double
                ) * 100
        end as trips_change_1w_vs_2w_pct,

        case
            when
                lag(total_trips__leaky, 104)
                    over (order by week_start_date)
                > 0
                then (
                    (
                        lag(total_trips__leaky, 52)
                            over (order by week_start_date)
                        - lag(total_trips__leaky, 104)
                            over (order by week_start_date)
                    )::double
                    / lag(total_trips__leaky, 104)
                        over (order by week_start_date)
                    ::double
                ) * 100
        end as trips_change_52w_vs_104w_pct,

        -- User mix lags (non-leaky)
        lag(annual_member_ratio__leaky, 1) over (
            order by week_start_date
        ) as annual_member_ratio_lag_1w,
        avg(annual_member_ratio__leaky) over (
            order by week_start_date rows between 4 preceding
            and 1 preceding
        ) as annual_member_ratio_rolling_4w_avg,

        -- Duration lags (non-leaky)
        lag(avg_trip_duration__leaky, 1)
            over (order by week_start_date)
            as avg_trip_duration_lag_1w,
        avg(avg_trip_duration__leaky)
            over (
                order by
                    week_start_date
                rows between 4 preceding and 1 preceding
            )
            as avg_trip_duration_rolling_4w_avg

    from weekly_base
)

select * from lagged_features
order by week_start_date
