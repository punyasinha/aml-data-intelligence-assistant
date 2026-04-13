{{
    config(
        materialized='table',
        tags=['marts', 'dimension', 'customers'],
        post_hook="alter table {{ this }} cluster by (risk_rating)"
    )
}}

/*
    Dimension: dim_customer
    Grain: one row per customer (current state — Type 1 SCD)

    Combines cleaned customer attributes with behavioural risk signals
    derived from int_customer_risk_profile. Designed for use as the
    primary customer conformed dimension across all AML marts.

    Analyst usage:
    - Slice transaction and alert facts by customer segment, risk rating, PEP status
    - Identify customers with high composite risk scores for enhanced due diligence
*/

with risk_profiles as (

    select * from {{ ref('int_customer_risk_profile') }}

),

final as (

    select
        -- surrogate key
        {{ dbt_utils.generate_surrogate_key(['customer_id']) }}    as customer_key,

        -- natural key
        customer_id,

        -- descriptive attributes
        full_name,
        customer_segment,
        country_of_birth,
        country_of_residence,
        onboarding_date,
        days_as_customer

    from risk_profiles

)

-- Re-join to staging to get fields not in intermediate
select
    f.customer_key,
    f.customer_id,
    f.full_name,
    f.customer_segment,
    f.country_of_birth,
    f.country_of_residence,
    f.onboarding_date,
    f.days_as_customer,
    s.annual_income_band,
    s.relationship_manager,
    s.date_of_birth,
    s.age_years,

    -- risk & compliance attributes
    rp.risk_rating,
    rp.kyc_status,
    rp.is_pep,
    rp.is_sanctioned,
    rp.composite_risk_score,
    case
        when rp.composite_risk_score >= 80 then 'CRITICAL'
        when rp.composite_risk_score >= 60 then 'HIGH'
        when rp.composite_risk_score >= 40 then 'ELEVATED'
        else 'STANDARD'
    end                                                            as enhanced_due_diligence_tier,

    -- behavioural signals
    rp.total_transaction_count,
    rp.total_transaction_volume_aud,
    rp.avg_transaction_amount_aud,
    rp.large_transaction_count,
    rp.near_threshold_count,
    rp.distinct_counterparty_countries,
    rp.total_alert_count,
    rp.open_alert_count,
    rp.escalated_alert_count,
    rp.fiu_referral_count,
    rp.last_transaction_date,

    -- metadata
    current_timestamp()                                            as _updated_at

from final f
inner join {{ ref('stg_customers') }} s
    on f.customer_id = s.customer_id
inner join {{ ref('int_customer_risk_profile') }} rp
    on f.customer_id = rp.customer_id
