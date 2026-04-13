-- Test: OPEN alerts must not have a resolution_date
-- An alert with status=OPEN and a populated resolution_date indicates
-- a pipeline or source system integrity issue.

select
    alert_id,
    alert_status,
    resolution_date,
    alert_date
from {{ ref('fct_aml_alerts') }}
where alert_status = 'OPEN'
  and resolution_date is not null
