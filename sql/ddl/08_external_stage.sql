
-- Run only when stage.mode = external.
-- The storage integration must already exist.

USE DATABASE RETAIL_DW;

CREATE STAGE IF NOT EXISTS BRONZE.STG_RETAIL_EXTERNAL
    URL = '{{ external_stage_url }}'
    STORAGE_INTEGRATION = {{ storage_integration }}
    FILE_FORMAT = BRONZE.FF_RETAIL_CSV
    DIRECTORY = (ENABLE = TRUE);
