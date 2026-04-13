-- Test: assert no negative transaction amounts exist in the mart
-- A negative amount_aud indicates a data quality issue in the source ledger extract.
-- All refunds and reversals should be represented as transaction_type = 'REVERSAL'
-- with a positive amount, not a negative value.

select
    transaction_id,
    amount_aud,
    transaction_type,
    transaction_status
from {{ ref('fct_transactions') }}
where amount_aud < 0
