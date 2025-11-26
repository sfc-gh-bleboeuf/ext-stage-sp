-- Test script for COPY_BETWEEN_STAGES procedure
-- This script calls the procedure with configurable source and target stages

USE DATABASE DOCUMENT_DB;
USE SCHEMA EXT_STAGE_DEMO;
-- ============================================================
-- CONFIGURATION VARIABLES - Set these before running
-- ============================================================
SET SOURCE_STAGE = '@SOURCE_STAGE';  -- Change to your external stage name (e.g., '@S3_SOURCE_STAGE')
SET TARGET_STAGE = '@TARGET_STAGE';  -- Change to your external stage name (e.g., '@S3_TARGET_STAGE')
SET FILE_PATTERN = '.txt';                 -- Change to match specific files (e.g., '*.csv')

-- ============================================================
-- Call the COPY_BETWEEN_STAGES procedure
-- ============================================================
CALL DOCUMENT_DB.EXT_STAGE_DEMO.COPY_BETWEEN_STAGES(
    $SOURCE_STAGE,
    $TARGET_STAGE,
    $FILE_PATTERN
;
