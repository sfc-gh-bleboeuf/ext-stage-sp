# PUT External Stage Demo

This demo demonstrates how to use Snowpark's file operation functionality to transfer files between Snowflake stages using a stored procedure.

## Overview

The demo creates:
- A database (`DOCUMENT_DB`) and schema (`EXT_STAGE_DEMO`)
- Two internal stages for testing (`SOURCE_STAGE` and `TARGET_STAGE`)
- A Snowpark stored procedure that transfers files between stages using Python file operations

## Prerequisites

- Snowflake account with `ACCOUNTADMIN` role access
- Snow CLI installed and configured

## File Structure

```
put_external_stage/
├── readme.md       # This documentation file
├── setup.sql       # Creates database, schema, stages, and procedures
├── run_demo.sql    # Sample usage and test queries
└── teardown.sql    # Cleanup script to remove demo objects
```

## Setup

### 1. Run the Setup Script

This will create:
- `DOCUMENT_DB` database (if not exists)
- `EXT_STAGE_DEMO` schema
- `SOURCE_STAGE` and `TARGET_STAGE` internal stages
- `PUT_FILE_TO_STAGE` stored procedure
- `COPY_BETWEEN_STAGES` stored procedure (alternative approach)

### 2. Upload a Test File (Optional)

To test the procedure, first upload a sample file to the source stage:

```bash
# Create a sample file
echo "Hello, this is a test file for the stage transfer demo." > /tmp/sample_file.txt

# Upload using Snow CLI
snow stage copy /tmp/sample_file.txt @DOCUMENT_DB.EXT_STAGE_DEMO.SOURCE_STAGE --connection <YOUR_CONNECTION>
```

Or via SQL:

```sql
PUT file:///tmp/sample_file.txt @DOCUMENT_DB.EXT_STAGE_DEMO.SOURCE_STAGE AUTO_COMPRESS=FALSE;
```

## Running the Demo

### Using the PUT_FILE_TO_STAGE Procedure

This procedure uses Snowpark's file operations to read a file from the source stage and write it to the target stage:

```bash
snow sql -q "CALL DOCUMENT_DB.EXT_STAGE_DEMO.PUT_FILE_TO_STAGE('@DOCUMENT_DB.EXT_STAGE_DEMO.SOURCE_STAGE', '@DOCUMENT_DB.EXT_STAGE_DEMO.TARGET_STAGE', 'sample_file.txt')" --connection <YOUR_CONNECTION>
```

### Using the COPY_BETWEEN_STAGES Procedure

Alternative procedure that lists and copies all matching files:

```bash
snow sql -q "CALL DOCUMENT_DB.EXT_STAGE_DEMO.COPY_BETWEEN_STAGES('@DOCUMENT_DB.EXT_STAGE_DEMO.SOURCE_STAGE', '@DOCUMENT_DB.EXT_STAGE_DEMO.TARGET_STAGE', '*')" --connection <YOUR_CONNECTION>
```

### Verify the Transfer

List files in both stages to verify the transfer:

```bash
# List source stage
snow sql -q "LIST @DOCUMENT_DB.EXT_STAGE_DEMO.SOURCE_STAGE" --connection <YOUR_CONNECTION>

# List target stage
snow sql -q "LIST @DOCUMENT_DB.EXT_STAGE_DEMO.TARGET_STAGE" --connection <YOUR_CONNECTION>
```

## Procedure Details

### PUT_FILE_TO_STAGE

| Parameter | Type | Description |
|-----------|------|-------------|
| `SOURCE_STAGE` | VARCHAR | Full stage path (e.g., `@DB.SCHEMA.STAGE_NAME`) |
| `TARGET_STAGE` | VARCHAR | Full stage path for destination |
| `FILE_NAME` | VARCHAR | Name of the file to transfer |

**How it works:**
1. Opens the file from source stage using `SnowflakeFile.open()`
2. Reads file content into memory
3. Creates an in-memory stream
4. Uploads to target stage using `session.file.put_stream()`

### COPY_BETWEEN_STAGES

| Parameter | Type | Description |
|-----------|------|-------------|
| `SOURCE_STAGE` | VARCHAR | Full stage path |
| `TARGET_STAGE` | VARCHAR | Full stage path for destination |
| `FILE_PATTERN` | VARCHAR | Pattern to match files (default: `*`) |

**How it works:**
1. Lists all files in source stage matching the pattern
2. Uses SQL `COPY FILES` command to copy each file

## Working with External Stages

To use this demo with external stages (S3, Azure Blob, GCS):

### 1. Create External Stage

```sql
-- Example: Create an S3 external stage
CREATE OR REPLACE STAGE MY_EXTERNAL_STAGE
    URL = 's3://my-bucket/path/'
    STORAGE_INTEGRATION = my_storage_integration;
```

### 2. Call the Procedure

```sql
CALL DOCUMENT_DB.EXT_STAGE_DEMO.PUT_FILE_TO_STAGE(
    '@MY_SOURCE_EXTERNAL_STAGE',
    '@MY_TARGET_EXTERNAL_STAGE',
    'my_file.csv'
);
```

## Teardown

To clean up all demo objects:

```bash
snow sql -f ./put_external_stage/teardown.sql --connection <YOUR_CONNECTION>
```

Or manually:

```bash
snow sql -q "DROP SCHEMA IF EXISTS DOCUMENT_DB.EXT_STAGE_DEMO CASCADE" --connection <YOUR_CONNECTION>
```

**Note:** The teardown script preserves the `DOCUMENT_DB` database. To drop the entire database:

```bash
snow sql -q "DROP DATABASE IF EXISTS DOCUMENT_DB" --connection <YOUR_CONNECTION>
```

## Troubleshooting

### Common Issues

1. **Permission Denied**: Ensure you have `ACCOUNTADMIN` role or appropriate privileges on stages
2. **File Not Found**: Verify the file exists in the source stage using `LIST @STAGE_NAME`
3. **Stage Not Found**: Use fully qualified stage names (e.g., `@DATABASE.SCHEMA.STAGE_NAME`)

### Debug Queries

```sql
-- Check procedure definition
DESCRIBE PROCEDURE DOCUMENT_DB.EXT_STAGE_DEMO.PUT_FILE_TO_STAGE(VARCHAR, VARCHAR, VARCHAR);

-- Check stage contents
LIST @DOCUMENT_DB.EXT_STAGE_DEMO.SOURCE_STAGE;

-- Check query history for errors
SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY())
WHERE QUERY_TEXT ILIKE '%PUT_FILE_TO_STAGE%'
ORDER BY START_TIME DESC
LIMIT 10;
```

## References

- [Snowpark Python File Operations](https://docs.snowflake.com/en/developer-guide/snowpark/python/working-with-files)
- [Snowflake Stages](https://docs.snowflake.com/en/user-guide/data-load-overview#staged-files)
- [Snow CLI Documentation](https://docs.snowflake.com/en/developer-guide/snowflake-cli-v2/index)

