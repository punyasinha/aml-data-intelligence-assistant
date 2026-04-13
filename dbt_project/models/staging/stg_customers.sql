{{
    config(
        materialized='view',
        tags=['staging', 'customers']
    )
}}

with source as (

    select * from {{ source('raw', 'raw_customers') }}

),

renamed as (

    select
        -- identifiers
        customer_id,

        -- personal details
        first_name,
        last_name,
        first_name || ' ' || last_name                  as full_name,
        try_to_date(date_of_birth, 'YYYY-MM-DD')        as date_of_birth,
        datediff('year', date_of_birth, current_date())  as age_years,
        country_of_birth,
        country_of_residence,

        -- risk & compliance
        upper(risk_rating)                               as risk_rating,
        upper(kyc_status)                                as kyc_status,
        try_to_date(kyc_review_date, 'YYYY-MM-DD')       as kyc_review_date,
        try_to_date(onboarding_date, 'YYYY-MM-DD')       as onboarding_date,
        datediff('day', onboarding_date, current_date()) as days_as_customer,
        case
            when upper(is_pep) = 'TRUE' then true
            else false
        end                                              as is_pep,
        case
            when upper(is_sanctioned) = 'TRUE' then true
            else false
        end                                              as is_sanctioned,

        -- segmentation
        upper(customer_segment)                          as customer_segment,
        annual_income_band,
        relationship_manager,

        -- metadata
        current_timestamp()                              as _loaded_at

    from source

)

select * from renamed
