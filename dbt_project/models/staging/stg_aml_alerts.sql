{{
    config(
        materialized='view',
        tags=['staging', 'aml', 'alerts']
    )
}}

with source as (

    select * from {{ source('raw', 'raw_aml_alerts') }}

),

renamed as (

    select
        -- identifiers
        alert_id,
        transaction_id,
        customer_id,

        -- alert classification
        upper(alert_type)                                 as alert_type,
        upper(rule_triggered)                             as rule_triggered,
        upper(status)                                     as alert_status,
        cast(risk_score as float)                         as risk_score,
        case
            when cast(risk_score as float) >= {{ var('alert_high_risk_threshold') }}
            then 'HIGH'
            when cast(risk_score as float) >= 60
            then 'MEDIUM'
            else 'LOW'
        end                                               as risk_band,

        -- assignment & resolution
        assigned_analyst,
        case
            when upper(escalated_to_fiu) = 'TRUE' then true
            else false
        end                                               as escalated_to_fiu,
        resolution_notes,

        -- dates
        try_to_date(alert_date, 'YYYY-MM-DD')             as alert_date,
        try_to_date(resolution_date, 'YYYY-MM-DD')        as resolution_date,
        case
            when resolution_date is not null
            then datediff(
                'day',
                try_to_date(alert_date, 'YYYY-MM-DD'),
                try_to_date(resolution_date, 'YYYY-MM-DD')
            )
        end                                               as days_to_resolve,

        -- metadata
        current_timestamp()                               as _loaded_at

    from source

)

select * from renamed
