-- ============================================================
-- Snowflake Setup — AML Data Intelligence Project
-- Run as ACCOUNTADMIN in a Snowflake worksheet
-- ============================================================


-- ============================================
-- 1. WAREHOUSE
-- ============================================
CREATE WAREHOUSE AML_WH
  WAREHOUSE_SIZE = 'X-SMALL'
  AUTO_SUSPEND = 60
  AUTO_RESUME = TRUE
  COMMENT = 'AML Data Intelligence Project';


-- ============================================
-- 2. DATABASE & SCHEMAS
-- ============================================
CREATE DATABASE AML_DB;

CREATE SCHEMA AML_DB.RAW;
CREATE SCHEMA AML_DB.REFERENCE;
CREATE SCHEMA AML_DB.STAGING;
CREATE SCHEMA AML_DB.MARTS;


-- ============================================
-- 3. ROLE
-- ============================================
CREATE ROLE TRANSFORMER;


-- ============================================
-- 4. SERVICE ACCOUNT USER (key-pair auth only)
-- ============================================
-- TYPE=SERVICE exempts the user from MFA and disables password auth entirely.
-- This is the standard pattern for CI/CD and automated tooling in enterprise.
-- Authentication uses RSA key-pair — no password exists for this user.
--
-- One-time local setup (run in terminal, not in Snowflake):
--   openssl genrsa 2048 | openssl pkcs8 -topk8 -inform PEM -out rsa_key.p8 -nocrypt
--   openssl rsa -in rsa_key.p8 -pubout -out rsa_key.pub
--   mkdir -p ~/.snowflake && mv rsa_key.p8 rsa_key.pub ~/.snowflake/
--
-- Then register the public key below (paste base64 content, no headers/footers):
CREATE USER SVC_DBT
  TYPE = SERVICE
  DEFAULT_ROLE = TRANSFORMER
  DEFAULT_WAREHOUSE = AML_WH
  COMMENT = 'Service account for dbt and data ingestion. Key-pair auth only.';

GRANT ROLE TRANSFORMER TO USER SVC_DBT;

ALTER USER SVC_DBT SET RSA_PUBLIC_KEY='<paste contents of rsa_key.pub here, excluding BEGIN/END lines>';


-- ============================================
-- 5. WAREHOUSE PERMISSIONS
-- ============================================
GRANT USAGE ON WAREHOUSE AML_WH TO ROLE TRANSFORMER;


-- ============================================
-- 6. DATABASE PERMISSIONS
-- ============================================
GRANT USAGE ON DATABASE AML_DB TO ROLE TRANSFORMER;
GRANT CREATE SCHEMA ON DATABASE AML_DB TO ROLE TRANSFORMER;


-- ============================================
-- 7. SCHEMA PERMISSIONS (existing schemas)
-- ============================================
GRANT USAGE ON ALL SCHEMAS IN DATABASE AML_DB TO ROLE TRANSFORMER;
GRANT CREATE TABLE ON ALL SCHEMAS IN DATABASE AML_DB TO ROLE TRANSFORMER;
GRANT CREATE VIEW ON ALL SCHEMAS IN DATABASE AML_DB TO ROLE TRANSFORMER;


-- ============================================
-- 8. TABLE/VIEW PERMISSIONS (existing objects)
-- ============================================
GRANT SELECT ON ALL TABLES IN DATABASE AML_DB TO ROLE TRANSFORMER;
GRANT SELECT ON ALL VIEWS IN DATABASE AML_DB TO ROLE TRANSFORMER;


-- ============================================
-- 9. FUTURE GRANTS (objects created after this point)
-- ============================================
GRANT SELECT ON FUTURE TABLES IN SCHEMA AML_DB.RAW TO ROLE TRANSFORMER;

GRANT ALL ON FUTURE TABLES IN SCHEMA AML_DB.STAGING TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE VIEWS IN SCHEMA AML_DB.STAGING TO ROLE TRANSFORMER;

GRANT ALL ON FUTURE TABLES IN SCHEMA AML_DB.MARTS TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE VIEWS IN SCHEMA AML_DB.MARTS TO ROLE TRANSFORMER;

GRANT ALL ON FUTURE TABLES IN SCHEMA AML_DB.REFERENCE TO ROLE TRANSFORMER;
GRANT ALL ON FUTURE VIEWS IN SCHEMA AML_DB.REFERENCE TO ROLE TRANSFORMER;


-- ============================================
-- 10. VERIFY
-- ============================================
SHOW GRANTS TO ROLE TRANSFORMER;
SHOW GRANTS TO USER SVC_DBT;
