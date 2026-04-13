{{
    config(
        materialized='view',
        tags=['staging', 'transactions']
    )
}}

with source as (

    select * from {{ source('raw', 'raw_transactions') }}

),

renamed as (

    select
        -- identifiers
        transaction_id,
        customer_id,
        reference_number,

        -- amounts
        cast(amount as float)                             as amount,
        upper(currency)                                   as currency,
        case
            when upper(currency) = 'AUD' then cast(amount as float)
            -- extend here with FX conversion for multi-currency
            else cast(amount as float)
        end                                               as amount_aud,

        -- flags for AML rules
        case
            when cast(amount as float) >= {{ var('transaction_large_amount_aud') }}
            then true else false
        end                                               as is_large_transaction,
        case
            when cast(amount as float) >= 9000
             and cast(amount as float) < {{ var('transaction_large_amount_aud') }}
            then true else false
        end                                               as is_near_threshold,

        -- transaction attributes
        upper(transaction_type)                           as transaction_type,
        upper(channel)                                    as channel,
        upper(merchant_category)                          as merchant_category,
        counterparty_country,
        upper(status)                                     as status,

        -- dates
        try_to_date(transaction_date, 'YYYY-MM-DD')       as transaction_date,
        date_part('year', try_to_date(transaction_date, 'YYYY-MM-DD'))   as transaction_year,
        date_part('month', try_to_date(transaction_date, 'YYYY-MM-DD'))  as transaction_month,
        date_part('dow', try_to_date(transaction_date, 'YYYY-MM-DD'))    as day_of_week,

        -- metadata
        current_timestamp()                               as _loaded_at

    from source

)

select * from renamed
