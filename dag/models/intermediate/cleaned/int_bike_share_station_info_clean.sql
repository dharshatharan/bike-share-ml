select
    station_id,
    name,
    lat,
    lon,
    capacity,
    -- altitude -- 45% null rest all 0
    is_charging_station,
    groups,
    obcn,
    short_name,
    nearby_distance,
    coalesce(physical_configuration, 'UNKNOWN') as physical_configuration,
    coalesce(address, name) as address,
    coalesce(rental_methods, []) as rental_methods
    -- _ride_code_support, -- all true, not useful
    -- rental_uris, -- all {}, not useful
    -- post_code, -- 29% null
    -- is_valet_station, -- 97% null
    -- cross_street -- 65% null
from {{ ref('stg_bike_share_station_info') }}
