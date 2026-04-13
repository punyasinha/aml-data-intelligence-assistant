{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'risk', 'customers']
    )
}}

/*
    Builds a composite risk profile per customer by aggregating transaction
    behaviour and alert history. Used by both fct_transactions and fct_aml_alerts
    to enrich downstream mart models with behavioural risk signals.

    Risk signals included:
    - Static: KYC risk rating, PEP status, sanctions flag
    - Behavioural: 30-day transaction volume, large transaction count, alert count
    - Derived: composite_risk_score (0–100)
*/

with customers as (

    select * from {{ ref('stg_customers') }}

),

transactions as (

    select * from {{ ref('stg_transactions') }}

),

alerts as (

    select * from {{ ref('stg_aml_alerts') }}

),

transaction_metrics as (

    select
        customer_id,
        count(*)                                                          as total_transaction_count,
        sum(amount_aud)                                                   as total_transaction_volume_aud,
        avg(amount_aud)                                                   as avg_transaction_amount_aud,
        max(amount_aud)                                                   as max_transaction_amount_aud,
        sum(case when is_large_transaction then 1 else 0 end)             as large_transaction_count,
        sum(case when is_near_threshold then 1 else 0 end)                as near_threshold_count,
        sum(case when transaction_type = 'DEBIT' then amount_aud else 0 end) as total_debit_aud,
        sum(case when transaction_type = 'CREDIT' then amount_aud else 0 end) as total_credit_aud,
        count(distinct counterparty_country)                              as distinct_counterparty_countries,
        max(transaction_date)                                             as last_transaction_date

    from transactions
    group by 1

),

alert_metrics as (

    select
        customer_id,
        count(*)                                                          as total_alert_count,
        sum(case when alert_status = 'OPEN' then 1 else 0 end)            as open_alert_count,
        sum(case when alert_status = 'ESCALATED' then 1 else 0 end)       as escalated_alert_count,
        sum(case when escalated_to_fiu then 1 else 0 end)                 as fiu_referral_count,
        max(risk_score)                                                   as max_alert_risk_score,
        avg(risk_score)                                                   as avg_alert_risk_score

    from alerts
    group by 1

),

combined as (

    select
        c.customer_id,
        c.full_name,
        c.risk_rating,
        c.kyc_status,
        c.is_pep,
        c.is_sanctioned,
        c.customer_segment,
        c.country_of_birth,
        c.country_of_residence,
        c.onboarding_date,
        c.days_as_customer,

        -- transaction signals
        coalesce(t.total_transaction_count, 0)          as total_transaction_count,
        coalesce(t.total_transaction_volume_aud, 0)     as total_transaction_volume_aud,
        coalesce(t.avg_transaction_amount_aud, 0)       as avg_transaction_amount_aud,
        coalesce(t.max_transaction_amount_aud, 0)       as max_transaction_amount_aud,
        coalesce(t.large_transaction_count, 0)          as large_transaction_count,
        coalesce(t.near_threshold_count, 0)             as near_threshold_count,
        coalesce(t.distinct_counterparty_countries, 0)  as distinct_counterparty_countries,
        t.last_transaction_date,

        -- alert signals
        coalesce(a.total_alert_count, 0)                as total_alert_count,
        coalesce(a.open_alert_count, 0)                 as open_alert_count,
        coalesce(a.escalated_alert_count, 0)            as escalated_alert_count,
        coalesce(a.fiu_referral_count, 0)               as fiu_referral_count,
        coalesce(a.max_alert_risk_score, 0)             as max_alert_risk_score,
        coalesce(a.avg_alert_risk_score, 0)             as avg_alert_risk_score,

        -- composite risk score (0–100)
        -- weighted combination of static + behavioural signals
        least(100, round(
            case c.risk_rating
                when 'HIGH'   then 40
                when 'MEDIUM' then 20
                else 10
            end
            + case when c.is_pep        then 15 else 0 end
            + case when c.is_sanctioned then 25 else 0 end
            + least(15, coalesce(a.total_alert_count, 0) * 3)
            + least(10, coalesce(a.escalated_alert_count, 0) * 5)
            + least(5,  coalesce(t.near_threshold_count, 0) * 2)
        , 0))                                           as composite_risk_score

    from customers c
    left join transaction_metrics t on c.customer_id = t.customer_id
    left join alert_metrics a on c.customer_id = a.customer_id

)

select * from combined
