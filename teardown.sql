-- =============================================================================
-- PUT External Stage Demo - Teardown Script
-- Removes all demo objects created by setup.sql
-- =============================================================================

-- Use ACCOUNTADMIN role for teardown
USE ROLE ACCOUNTADMIN;

-- Set the context
USE DATABASE DOCUMENT_DB;

-- =============================================================================
-- Drop the demo schema and all objects within it
-- CASCADE will drop all stages, procedures, etc. in the schema
-- =============================================================================

DROP SCHEMA IF EXISTS EXT_STAGE_DEMO CASCADE;

-- =============================================================================
-- Verification
-- =============================================================================

-- Verify schema is dropped
SHOW SCHEMAS IN DATABASE DOCUMENT_DB;

SELECT 'Teardown completed successfully! Schema EXT_STAGE_DEMO has been removed.' AS STATUS;

-- =============================================================================
-- Optional: Uncomment below to drop the entire database
-- WARNING: This will remove ALL objects in DOCUMENT_DB
-- =============================================================================

-- DROP DATABASE IF EXISTS DOCUMENT_DB;

