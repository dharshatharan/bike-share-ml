select
    "Trip Id"::bigint as trip_id,
    "Trip  Duration"::bigint as trip_duration,
    "Start Station Id"::bigint as start_station_id,
    strptime("Start Time", '%m/%d/%Y %H:%M')::timestamp as start_time,
    "Start Station Name"::varchar as start_station_name,
    "End Station Id"::varchar as end_station_id,
    strptime("End Time", '%m/%d/%Y %H:%M')::timestamp as end_time,
    "End Station Name"::varchar as end_station_name,
    "Bike Id"::bigint as bike_id,
    "User Type"::varchar as user_type
from {{ source('bikeshare', 'bike_share_trips') }}
