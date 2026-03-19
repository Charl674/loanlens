-- models/staging/stg_delinquency_rates.sql
-- Cleans and types the raw FRED data for use by mart models
-- Materialised as a VIEW (no storage cost, always current)

with source as (
    select * from {{ source('fred_raw', 'FRED_DELINQUENCY_RAW') }}
),

cleaned as (

    select
        lower(series_id)                                        as series_id,
        trim(series_name)                                       as series_name,

        case lower(series_id)
            when 'drclacbs'      then 'Consumer Loans'
            when 'drblacbs'      then 'Business Loans'
            when 'drcclacbs'     then 'Credit Cards'
            when 'drcrelexfacbs' then 'Commercial Real Estate'
            when 'drsfrmacbs'    then 'Mortgages'
            when 'corblacbs'     then 'Business Loans'
            when 'corcacbs'      then 'Credit Cards'
            when 'fedfunds'      then 'Macro Indicator'
            else 'Unknown'
        end                                                     as loan_category,

        case
            when left(upper(series_id), 2) = 'DR' then 'delinquency_rate'
            when left(upper(series_id), 2) = 'CO' then 'charge_off_rate'
            else 'macro_indicator'
        end                                                     as metric_type,

        try_to_date(observation_date, 'YYYY-MM-DD')            as observation_date,
        year(try_to_date(observation_date, 'YYYY-MM-DD'))      as observation_year,
        quarter(try_to_date(observation_date, 'YYYY-MM-DD'))   as observation_quarter,

        year(try_to_date(observation_date, 'YYYY-MM-DD')) || '-Q' ||
        quarter(try_to_date(observation_date, 'YYYY-MM-DD'))   as year_quarter,

        try_to_double(value)                                    as rate_value,
        (value = '.')                                           as is_missing,

        trim(units)                                             as units,
        trim(frequency)                                         as frequency,
        loaded_at                                               as loaded_at

    from source
    where try_to_date(observation_date, 'YYYY-MM-DD') is not null

)

select * from cleaned


