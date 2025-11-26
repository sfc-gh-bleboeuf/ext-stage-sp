-- =============================================================================
-- PUT External Stage Demo - Setup Script
-- Creates database, schema, stages, and stored procedure for file operations
-- =============================================================================

-- Use ACCOUNTADMIN role for setup
USE ROLE ACCOUNTADMIN;

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS DOCUMENT_DB;

-- Use the database
USE DATABASE DOCUMENT_DB;

-- Create the demo schema
CREATE SCHEMA IF NOT EXISTS EXT_STAGE_DEMO;

-- Use the schema
USE SCHEMA EXT_STAGE_DEMO;

-- Set warehouse
USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- Create demo stages for testing (internal stages for demonstration)
-- In production, you would use your actual external stages
-- =============================================================================

-- Source stage - where files will be read from
CREATE STAGE IF NOT EXISTS SOURCE_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Source stage for file transfer demo';

-- Target stage - where files will be written to
CREATE STAGE IF NOT EXISTS TARGET_STAGE
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Target stage for file transfer demo';

-- =============================================================================
-- Create the Snowpark stored procedure for file transfer
-- =============================================================================

CREATE OR REPLACE PROCEDURE EXT_STAGE_DEMO.PUT_FILE_TO_STAGE(
    SOURCE_STAGE VARCHAR,
    TARGET_STAGE VARCHAR,
    FILE_NAME VARCHAR
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'put_file_to_stage'
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark
from snowflake.snowpark.files import SnowflakeFile

def put_file_to_stage(session: snowpark.Session, source_stage: str, target_stage: str, file_name: str) -> str:
    """
    Transfer a file from source stage to target stage using Snowpark file operations.
    
    Args:
        session: Snowpark session object
        source_stage: Source stage path (e.g., '@SOURCE_STAGE')
        target_stage: Target stage path (e.g., '@TARGET_STAGE')
        file_name: Name of the file to transfer
        
    Returns:
        Status message indicating success or failure
    """
    import io
    
    try:
        # Construct full source path
        source_path = f"{source_stage}/{file_name}"
        
        # Read file content from source stage using get_stream
        # This streams the file directly without writing to local disk
        with SnowflakeFile.open(source_path, 'rb', require_scoped_url=False) as source_file:
            file_content = source_file.read()
        
        # Create an in-memory stream from the content
        file_stream = io.BytesIO(file_content)
        
        # Upload to target stage using put_stream
        # Construct target file name
        target_file_name = file_name
        
        # Use session.file.put_stream to upload
        result = session.file.put_stream(
            input_stream=file_stream,
            stage_location=f"{target_stage}/{target_file_name}",
            auto_compress=False,
            overwrite=True
        )
        
        return f"SUCCESS: File '{file_name}' transferred from {source_stage} to {target_stage}"
        
    except Exception as e:
        return f"ERROR: Failed to transfer file - {str(e)}"
$$;

-- =============================================================================
-- Alternative procedure using SQL COPY for stage-to-stage transfer
-- This approach may be more efficient for large files or bulk operations
-- =============================================================================

CREATE OR REPLACE PROCEDURE EXT_STAGE_DEMO.COPY_BETWEEN_STAGES(
    SOURCE_STAGE VARCHAR,
    TARGET_STAGE VARCHAR,
    FILE_PATTERN VARCHAR DEFAULT '*'
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'copy_between_stages'
EXECUTE AS CALLER
AS
$$
import snowflake.snowpark as snowpark

def copy_between_stages(session: snowpark.Session, source_stage: str, target_stage: str, file_pattern: str = '*') -> str:
    """
    Copy files from source stage to target stage using COPY command.
    
    Args:
        session: Snowpark session object
        source_stage: Source stage name (e.g., '@SOURCE_STAGE')
        target_stage: Target stage name (e.g., '@TARGET_STAGE')
        file_pattern: Pattern to match files (default: '*' for all files)
        
    Returns:
        Status message with details of the operation
    """
    try:
        # List files in source stage
        list_query = f"LIST {source_stage} PATTERN = '.*{file_pattern}.*'"
        files_df = session.sql(list_query).collect()
        
        if not files_df:
            return f"No files found in {source_stage} matching pattern '{file_pattern}'"
        
        transferred_files = []
        
        for row in files_df:
            # Extract file name from the stage path
            file_path = row['name']
            file_name = file_path.split('/')[-1]
            
            # Use COPY command to copy file to target stage
            copy_query = f"""
                COPY FILES
                INTO {target_stage}
                FROM {source_stage}
                FILES = ('{file_name}')
            """
            
            try:
                session.sql(copy_query).collect()
                transferred_files.append(file_name)
            except Exception as copy_error:
                # If COPY FILES not supported, try alternative approach
                return f"Note: COPY FILES command may require specific stage types. Error: {str(copy_error)}"
        
        return f"SUCCESS: Transferred {len(transferred_files)} file(s): {', '.join(transferred_files)}"
        
    except Exception as e:
        return f"ERROR: {str(e)}"
$$;

-- =============================================================================
-- Grant necessary permissions (adjust as needed for your use case)
-- =============================================================================

GRANT USAGE ON DATABASE DOCUMENT_DB TO ROLE ACCOUNTADMIN;
GRANT USAGE ON SCHEMA EXT_STAGE_DEMO TO ROLE ACCOUNTADMIN;
-- For internal stages, use READ/WRITE instead of USAGE
GRANT READ, WRITE ON STAGE EXT_STAGE_DEMO.SOURCE_STAGE TO ROLE ACCOUNTADMIN;
GRANT READ, WRITE ON STAGE EXT_STAGE_DEMO.TARGET_STAGE TO ROLE ACCOUNTADMIN;
GRANT USAGE ON PROCEDURE EXT_STAGE_DEMO.PUT_FILE_TO_STAGE(VARCHAR, VARCHAR, VARCHAR) TO ROLE ACCOUNTADMIN;
GRANT USAGE ON PROCEDURE EXT_STAGE_DEMO.COPY_BETWEEN_STAGES(VARCHAR, VARCHAR, VARCHAR) TO ROLE ACCOUNTADMIN;

-- =============================================================================
-- Verification queries
-- =============================================================================

-- Show created objects
SHOW STAGES IN SCHEMA EXT_STAGE_DEMO;
SHOW PROCEDURES IN SCHEMA EXT_STAGE_DEMO;

SELECT 'Setup completed successfully!' AS STATUS;

