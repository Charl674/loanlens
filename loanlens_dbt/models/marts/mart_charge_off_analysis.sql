-- models/marts/mart_charge_off_analysis.sql
-- Compares charge-off rates against delinquency rates
-- Shows the lag between delinquency and actual losses

with staging as (
    select * from {{ ref('stg_delinquency_rates') }}
),

charge_offs as (
    select
        observation_date,
        observation_year,
        observation_quarter,
        year_quarter,
        loan_category,
        series_id,
        rate_value          as charge_off_rate
    from staging
    where metric_type = 'charge_off_rate'
      and is_missing = false
      and rate_value is not null
),

delinquency as (
    select
        observation_date,
        loan_category,
        rate_value          as delinquency_rate
    from staging
    where metric_type = 'delinquency_rate'
      and is_missing = false
      and rate_value is not null
),

combined as (
    select
        c.observation_date,
        c.observation_year,
        c.observation_quarter,
        c.year_quarter,
        c.loan_category,
        c.series_id,
        c.charge_off_rate,
        d.delinquency_rate,
        -- Spread between delinquency and charge-off
        -- Positive spread = delinquency leading charge-offs (early warning signal)
        d.delinquency_rate - c.charge_off_rate   as delinquency_charge_off_spread,
        -- Quarter over quarter change in charge-off rate
        c.charge_off_rate - lag(c.charge_off_rate) over (
            partition by c.series_id
            order by c.observation_date
        )                                         as qoq_change
    from charge_offs c
    left join delinquency d
        on c.observation_date = d.observation_date
        and c.loan_category   = d.loan_category
)

select * from combined
order by observation_date desc, loan_category