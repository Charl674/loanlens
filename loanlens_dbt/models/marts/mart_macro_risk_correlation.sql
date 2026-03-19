-- models/marts/mart_macro_risk_correlation.sql
-- Correlates Fed Funds Rate with delinquency and charge-off rates
-- Shows how Fed rate hikes flow through to loan defaults

with staging as (
    select * from {{ ref('stg_delinquency_rates') }}
),

fed_funds as (
    select
        observation_date,
        observation_year,
        observation_quarter,
        year_quarter,
        rate_value      as fed_funds_rate
    from staging
    where series_id = 'fedfunds'
      and is_missing = false
      and rate_value is not null
),

credit_rates as (
    select
        observation_date,
        observation_year,
        observation_quarter,
        year_quarter,
        loan_category,
        series_id,
        metric_type,
        rate_value
    from staging
    where metric_type in ('delinquency_rate', 'charge_off_rate')
      and is_missing = false
      and rate_value is not null
),

combined as (
    select
        cr.observation_date,
        cr.observation_year,
        cr.observation_quarter,
        cr.year_quarter,
        cr.loan_category,
        cr.series_id,
        cr.metric_type,
        cr.rate_value,
        ff.fed_funds_rate,
        -- Rate differential — how much higher is delinquency vs fed funds
        cr.rate_value - ff.fed_funds_rate           as spread_vs_fed_funds,
        -- Fed funds rate 4 quarters ago (lagged) — rate hikes take ~1 year to show in defaults
        lag(ff.fed_funds_rate, 4) over (
            partition by cr.series_id
            order by cr.observation_date
        )                                           as fed_funds_rate_1yr_ago,
        -- Change in fed funds over past year
        ff.fed_funds_rate - lag(ff.fed_funds_rate, 4) over (
            partition by cr.series_id
            order by cr.observation_date
        )                                           as fed_funds_yoy_change
    from credit_rates cr
    left join fed_funds ff
        on cr.observation_date = ff.observation_date
)

select * from combined
order by observation_date desc, loan_category, metric_type