{{
    config(
        materialized='table',
        tags=['marts', 'fact', 'transactions', 'aml'],
        post_hook="alter table {{ this }} cluster by (date_id, customer_key)"
    )
}}

/*
    Fact: fct_transactions
    Grain: one row per financial transaction

    Core transaction fact enriched with:
    - Customer dimension key for joins
    - AML rule flags from int_flagged_transactions
    - Alert linkage (whether an alert exists for this transaction)

    Primary use cases:
    - AML transaction monitoring dashboards
    - Analyst review queues
    - AUSTRAC regulatory reporting (threshold transactions)
    - Customer risk profiling and behavioural analytics
*/

with transactions as (

    select * from {{ ref('int_flagged_transactions') }}

),

customers as (

    select
        customer_id,
        customer_key,
        risk_rating,
        kyc_status,
        is_pep,
        is_sanctioned,
        composite_risk_score,
        enhanced_due_diligence_tier

    from {{ ref('dim_customer') }}

),

dates as (

    select date_id, fiscal_year, fiscal_month, quarter_label
    from {{ ref('dim_date') }}

),

alerts as (

    select
        transaction_id,
        alert_id,
        alert_status,
        risk_score       as alert_risk_score,
        escalated_to_fiu

    from {{ ref('stg_aml_alerts') }}

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['t.transaction_id']) }}  as transaction_key,

        -- foreign keys (Kimball FK pattern)
        t.transaction_id,
        c.customer_key,
        t.transaction_date                                            as date_id,

        -- transaction measures
        t.amount_aud,
        t.transaction_type,
        t.channel,
        t.counterparty_country,
        t.status                                                      as transaction_status,

        -- AML rule flags
        t.flag_large_cash,
        t.flag_structuring,
        t.flag_high_risk_country,
        t.flag_pep_customer,
        t.flag_sanctioned_customer,
        t.rules_triggered_count,
        t.is_flagged,

        -- customer risk context (denormalised for analytics convenience)
        c.risk_rating                                                 as customer_risk_rating,
        c.kyc_status                                                  as customer_kyc_status,
        c.composite_risk_score,
        c.enhanced_due_diligence_tier,

        -- alert linkage
        a.alert_id,
        a.alert_status,
        a.alert_risk_score,
        a.escalated_to_fiu,
        case when a.alert_id is not null then true else false end      as has_alert,

        -- fiscal context
        d.fiscal_year,
        d.fiscal_month,
        d.quarter_label,

        -- metadata
        current_timestamp()                                           as _updated_at

    from transactions t
    left join customers c
        on t.customer_id = c.customer_id
    left join alerts a
        on t.transaction_id = a.transaction_id
    left join dates d
        on t.transaction_date = d.date_id

)

select * from final
