{{
    config(
        materialized='ephemeral',
        tags=['intermediate', 'aml', 'transactions']
    )
}}

/*
    Identifies transactions that meet one or more AML rule criteria.
    Annotates each transaction with the specific flags triggered, which
    feeds into fct_transactions for analyst dashboards and rule reporting.

    Rules implemented:
    - RULE_LCT_001: Large Cash Transaction (>= AUD 10,000)
    - RULE_STR_002: Structuring (near-threshold, >= AUD 9,000)
    - RULE_HRC_003: High-Risk Country counterparty
    - RULE_PEP_004: Transaction involving a PEP customer
    - RULE_SAN_005: Transaction involving a sanctioned customer
*/

with transactions as (

    select * from {{ ref('stg_transactions') }}

),

customers as (

    select
        customer_id,
        risk_rating,
        is_pep,
        is_sanctioned,
        kyc_status

    from {{ ref('stg_customers') }}

),

-- FATF / AUSTRAC high-risk jurisdictions — sourced from ref_high_risk_countries seed.
-- Update the seed CSV to add/remove countries without touching this model.
high_risk_countries as (

    select country_name
    from {{ ref('ref_high_risk_countries') }}

),

flagged as (

    select
        t.transaction_id,
        t.customer_id,
        t.transaction_date,
        t.amount_aud,
        t.transaction_type,
        t.channel,
        t.counterparty_country,
        t.status,

        -- rule flags
        t.is_large_transaction                                    as flag_large_cash,
        t.is_near_threshold                                       as flag_structuring,
        case
            when hrc.country_name is not null then true
            else false
        end                                                       as flag_high_risk_country,
        case when c.is_pep        then true else false end        as flag_pep_customer,
        case when c.is_sanctioned then true else false end        as flag_sanctioned_customer,

        -- count of rules triggered
        (
            case when t.is_large_transaction then 1 else 0 end
          + case when t.is_near_threshold then 1 else 0 end
          + case when hrc.country_name is not null then 1 else 0 end
          + case when c.is_pep then 1 else 0 end
          + case when c.is_sanctioned then 1 else 0 end
        )                                                         as rules_triggered_count,

        -- overall flag
        case
            when t.is_large_transaction
              or t.is_near_threshold
              or hrc.country_name is not null
              or c.is_pep
              or c.is_sanctioned
            then true
            else false
        end                                                       as is_flagged,

        c.risk_rating                                             as customer_risk_rating,
        c.kyc_status                                              as customer_kyc_status

    from transactions t
    left join customers c
        on t.customer_id = c.customer_id
    left join high_risk_countries hrc
        on t.counterparty_country = hrc.country_name

)

select * from flagged
