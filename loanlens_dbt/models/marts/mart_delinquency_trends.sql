-- models/marts/mart_delinquency_trends.sql
-- Wide-format quarterly delinquency table with trend calculations

with stg as (

    select *
    from {{ ref('stg_delinquency_rates') }}
    where is_missing = false
      and metric_type = 'delinquency_rate'

),

pivoted as (

    select
        year_quarter,
        min(observation_date) as observation_date,

        max(case when series_id = 'drclacbs'      then rate_value end) as consumer_delinquency_rate,
        max(case when series_id = 'drblacbs'      then rate_value end) as business_delinquency_rate,
        max(case when series_id = 'drcclacbs'     then rate_value end) as credit_card_delinquency_rate,
        max(case when series_id = 'drcrelexfacbs' then rate_value end) as cre_delinquency_rate,
        max(case when series_id = 'drsfrmacbs'    then rate_value end) as mortgage_delinquency_rate

    from stg
    group by year_quarter

),

with_trends as (

    select
        *,

        consumer_delinquency_rate - lag(consumer_delinquency_rate, 1)
            over (order by observation_date)                            as consumer_qoq_change,

        credit_card_delinquency_rate - lag(credit_card_delinquency_rate, 1)
            over (order by observation_date)                            as credit_card_qoq_change,

        greatest(
            coalesce(consumer_delinquency_rate, 0),
            coalesce(business_delinquency_rate, 0),
            coalesce(credit_card_delinquency_rate, 0),
            coalesce(cre_delinquency_rate, 0),
            coalesce(mortgage_delinquency_rate, 0)
        )                                                               as max_rate_this_quarter,

        round(
            (
                coalesce(consumer_delinquency_rate, 0) +
                coalesce(business_delinquency_rate, 0) +
                coalesce(credit_card_delinquency_rate, 0) +
                coalesce(cre_delinquency_rate, 0) +
                coalesce(mortgage_delinquency_rate, 0)
            ) / 5.0,
        3)                                                              as portfolio_avg_delinquency

    from pivoted

)

select *
from with_trends
order by observation_date desc