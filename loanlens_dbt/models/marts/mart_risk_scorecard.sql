-- models/marts/mart_risk_scorecard.sql
-- Composite risk score and RAG status for each loan category

with stg as (

    select *
    from {{ ref('stg_delinquency_rates') }}
    where is_missing = false
      and metric_type = 'delinquency_rate'

),

rolling_stats as (

    select
        year_quarter,
        observation_date,
        loan_category,
        rate_value as current_rate,

        avg(rate_value)
            over (
                partition by loan_category
                order by observation_date
                rows between 4 preceding and 1 preceding
            ) as rolling_4q_avg,

        avg(rate_value)
            over (
                partition by loan_category
                order by observation_date
                rows between 8 preceding and 1 preceding
            ) as rolling_8q_avg

    from stg

),

scored as (

    select
        *,

        round(
            (current_rate - rolling_4q_avg) /
            nullif(rolling_4q_avg, 0) * 100,
        1) as pct_above_4q_avg,

        least(100, greatest(0,
            case
                when current_rate >= 8.0 then 70
                when current_rate >= 5.0 then 50
                when current_rate >= 3.0 then 30
                else 10
            end
            + coalesce(
                round((current_rate - rolling_4q_avg) / 5.0 * 10, 0),
            0)
        )) as risk_score

    from rolling_stats

),

final as (

    select
        *,
        case
            when risk_score >= 70 then 'RED'
            when risk_score >= 40 then 'AMBER'
            else 'GREEN'
        end as rag_status,

        (observation_date = max(observation_date) over ()) as is_current_quarter

    from scored

)

select *
from final
order by loan_category, observation_date desc