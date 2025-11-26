-- =============================================================================
-- PUT External Stage Demo - Run Demo Script
-- Sample usage and test queries
-- =============================================================================

-- Use ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;
USE DATABASE DOCUMENT_DB;
USE SCHEMA EXT_STAGE_DEMO;
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- Step 1: Verify stages exist
-- =============================================================================

SHOW STAGES;

-- =============================================================================
-- Step 2: List current contents of stages
-- =============================================================================

-- List source stage contents
LIST @SOURCE_STAGE;

-- List target stage contents  
LIST @TARGET_STAGE;

-- =============================================================================
-- Step 3: Upload a test file to source stage (if needed)
-- Note: Run this from Snow CLI or SnowSQL client
-- =============================================================================

-- Create a sample file first (run from terminal):
-- echo "Sample content for stage transfer demo" > /tmp/test_file.txt

-- Then upload (uncomment and run from client that supports PUT):
-- PUT file:///tmp/test_file.txt @SOURCE_STAGE AUTO_COMPRESS=FALSE OVERWRITE=TRUE;

-- =============================================================================
-- Step 4: Call the stored procedure to transfer file
-- =============================================================================

-- Transfer a specific file from source to target stage
CALL PUT_FILE_TO_STAGE(
    '@DOCUMENT_DB.EXT_STAGE_DEMO.SOURCE_STAGE',
    '@DOCUMENT_DB.EXT_STAGE_DEMO.TARGET_STAGE',
    'test_file.txt'
);

-- =============================================================================
-- Step 5: Alternative - Use the bulk copy procedure
-- =============================================================================

-- Copy all files matching pattern
CALL COPY_BETWEEN_STAGES(
    '@DOCUMENT_DB.EXT_STAGE_DEMO.SOURCE_STAGE',
    '@DOCUMENT_DB.EXT_STAGE_DEMO.TARGET_STAGE',
    '*'
);

-- =============================================================================
-- Step 6: Verify transfer was successful
-- =============================================================================

-- List target stage to see transferred files
LIST @TARGET_STAGE;

-- =============================================================================
-- Step 7: Show procedure definitions
-- =============================================================================

SHOW PROCEDURES IN SCHEMA EXT_STAGE_DEMO;

-- Describe the main procedure
DESCRIBE PROCEDURE PUT_FILE_TO_STAGE(VARCHAR, VARCHAR, VARCHAR);

-- =============================================================================
-- Example with External Stages (modify as needed for your environment)
-- =============================================================================

/*
-- Create external stages (example with S3)
CREATE OR REPLACE STAGE MY_S3_SOURCE
    URL = 's3://my-source-bucket/data/'
    STORAGE_INTEGRATION = my_s3_integration;

CREATE OR REPLACE STAGE MY_S3_TARGET
    URL = 's3://my-target-bucket/data/'
    STORAGE_INTEGRATION = my_s3_integration;

-- Transfer file between external stages
CALL PUT_FILE_TO_STAGE(
    '@MY_S3_SOURCE',
    '@MY_S3_TARGET',
    'data_file.csv'
);
*/

-- =============================================================================
-- Cleanup test files (optional)
-- =============================================================================

-- Remove files from target stage
-- REMOVE @TARGET_STAGE PATTERN='.*';

SELECT 'Demo execution completed!' AS STATUS;

