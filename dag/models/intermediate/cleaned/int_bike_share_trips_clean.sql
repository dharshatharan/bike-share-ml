select
    trip_id,
    trip_duration,
    start_station_id,
    start_time,
    start_station_name,
    end_station_id,
    end_time,
    end_station_name,
    bike_id,
    user_type
from {{ ref('stg_bike_share_trips') }}
-- all the trips without a duration are trips that have the same start and end station and start and end time
-- so we can ignore them
where trip_duration is not null