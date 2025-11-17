select
    event::varchar as holiday_name,
    date::varchar as date_string,
    strptime(date, '%d/%m/%Y')::date as holiday_date
from {{ ref('seed_holidays') }}
