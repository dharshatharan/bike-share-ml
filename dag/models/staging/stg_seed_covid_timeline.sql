select
    dates::varchar as date_string,
    strptime(dates, '%b, %Y')::date as report_date,
    restrictions::boolean as restrictions,
    replace(hospitalizations, ',', '')::bigint as hospitalizations,
    replace(icu_admissions, ',', '')::bigint as icu_admissions,
    replace(inhospital_deaths, ',', '')::bigint as inhospital_deaths,
    replace(ed_visits, ',', '')::bigint as ed_visits
from {{ ref('seed_covid_timeline') }}
