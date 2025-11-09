select
    station_id,
    name,
    coalesce(physical_configuration, 'UNKNOWN') as physical_configuration,
    lat,
    lon,
    -- altitude -- 45% null rest all 0
    coalesce(address, name) as address,
    capacity,
    is_charging_station,
    coalesce(rental_methods, []) as rental_methods,
    groups,
    obcn,
    short_name,
    nearby_distance,
    -- _ride_code_support, -- all true, not useful
    -- rental_uris, -- all {}, not useful
    -- post_code, -- 29% null
    -- is_valet_station, -- 97% null
    -- cross_street -- 65% null
from {{ ref('stg_bike_share_station_info') }}