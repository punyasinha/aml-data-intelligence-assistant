{{
    config(
        materialized='table',
        tags=['marts', 'fact', 'aml', 'alerts'],
        post_hook="alter table {{ this }} cluster by (alert_date, alert_status)"
    )
}}

/*
    Fact: fct_aml_alerts
    Grain: one row per AML alert

    Enriched alert fact combining:
    - Alert metadata and classification from stg_aml_alerts
    - Transaction context from fct_transactions
    - Customer risk profile from dim_customer

    Primary use cases:
    - AML analyst workqueue management
    - Alert SLA tracking (days to resolve)
    - FIU referral and SAR reporting
    - Compliance dashboards — open/escalated alert counts by risk tier
    - Rule effectiveness analysis (which rules generate most actionable alerts)
*/

with alerts as (

    select * from {{ ref('stg_aml_alerts') }}

),

transactions as (

    select
        transaction_id,
        transaction_key,
        customer_key,
        amount_aud,
        transaction_type,
        channel,
        counterparty_country,
        transaction_status,
        flag_large_cash,
        flag_structuring,
        flag_high_risk_country,
        rules_triggered_count,
        fiscal_year,
        fiscal_month,
        quarter_label

    from {{ ref('fct_transactions') }}

),

customers as (

    select
        customer_id,
        customer_key,
        full_name,
        risk_rating,
        kyc_status,
        is_pep,
        is_sanctioned,
        composite_risk_score,
        enhanced_due_diligence_tier,
        customer_segment,
        country_of_residence

    from {{ ref('dim_customer') }}

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['a.alert_id']) }}       as alert_key,

        -- natural keys
        a.alert_id,
        a.transaction_id,
        a.customer_id,

        -- foreign keys
        t.customer_key,
        a.alert_date                                                  as date_id,

        -- alert classification
        a.alert_type,
        a.rule_triggered,
        a.alert_status,
        a.risk_score,
        a.risk_band,

        -- assignment & workflow
        a.assigned_analyst,
        a.escalated_to_fiu,
        a.resolution_notes,
        a.alert_date,
        a.resolution_date,
        a.days_to_resolve,

        -- SLA flags (target: OPEN < 5 days, ESCALATED same day)
        case
            when a.alert_status = 'OPEN'
             and datediff('day', a.alert_date, current_date()) > 5
            then true else false
        end                                                           as is_breached_sla,
        datediff('day', a.alert_date, current_date())                as alert_age_days,

        -- transaction context
        t.amount_aud                                                  as transaction_amount_aud,
        t.transaction_type,
        t.channel,
        t.counterparty_country,
        t.transaction_status,
        t.flag_large_cash,
        t.flag_structuring,
        t.flag_high_risk_country,
        t.rules_triggered_count,

        -- customer context
        c.full_name                                                   as customer_full_name,
        c.risk_rating                                                 as customer_risk_rating,
        c.kyc_status                                                  as customer_kyc_status,
        c.is_pep,
        c.is_sanctioned,
        c.composite_risk_score,
        c.enhanced_due_diligence_tier,
        c.customer_segment,
        c.country_of_residence,

        -- fiscal context
        t.fiscal_year,
        t.fiscal_month,
        t.quarter_label,

        -- metadata
        current_timestamp()                                           as _updated_at

    from alerts a
    left join transactions t
        on a.transaction_id = t.transaction_id
    left join customers c
        on a.customer_id = c.customer_id

)

select * from final
