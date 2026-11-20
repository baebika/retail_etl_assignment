# include/ingestion.py

import re
from pathlib import PurePosixPath

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


CONNECTION_ID = "snowflake_default"

DATABASE = "RETAIL_DW"
STAGE = f"{DATABASE}.BRONZE.STG_RETAIL_INTERNAL"
FILE_FORMAT = f"{DATABASE}.BRONZE.FF_RETAIL_CSV"

MANIFEST = f"{DATABASE}.BRONZE.STG_OPS_INGEST_MANIFEST"
BATCH_TABLE = f"{DATABASE}.OPS.OPS_ETL_BATCH"

# CSV column order must match the source files.
SOURCES = {
    "item_master": {
        "prefix": "Item_master",
        "table": f"{DATABASE}.BRONZE.STG_ITEM_MASTER",
        "source_name": "ITEM_MASTER",
        "columns": [
            "ITEM_ID", "ITEM_NAME", "BRAND", "CATEGORY_ID", "CATEGORY_NAME"
        ],
    },
    "store_master": {
    "prefix": "Store_master",
    "table": f"{DATABASE}.BRONZE.STG_STORE_MASTER",
    "source_name": "STORE_MASTER",
    "columns": [
        "STORE_ID",
        "STORE_NAME",
        "CITY",
        "DISTRICT_ID",
        "DISTRICT_NAME",
        "PROVINCE"
        ], 
        "positions": [1, 2, 3, 7, 8, 4],
    },
    "pos_sales": {
    "prefix": "Pos_sales",
    "table": f"{DATABASE}.BRONZE.STG_POS_SALES",
    "source_name": "POS_SALES",
    "columns": [
        "TXN_ID",
        "LINE_NO",
        "TXN_TS",
        "STORE_ID",
        "REGISTER_ID",
        "ITEM_ID",
        "QUANTITY",
        "UNIT_PRICE",
        "DISCOUNT_AMT",
        "TAX_AMT",
        "NET_AMT",
        ],
    },
    "inventory_snapshot": {
        "prefix": "Inventory_snapshot",
        "table": f"{DATABASE}.BRONZE.STG_INVENTORY_SNAPSHOT",
        "source_name": "INVENTORY_SNAPSHOT",
        "columns": [
            "SNAPSHOT_DATE", "STORE_ID", "ITEM_ID", "QTY_ON_HAND"
        ],
    },
}


def sql_string(value):
    """Safely quote a string literal for SQL."""
    return "'" + str(value).replace("'", "''") + "'"


def get_hook():
    return SnowflakeHook(snowflake_conn_id=CONNECTION_ID)


def create_batch(business_date, dag_id, run_id):
    """Create one batch record for this DAG run and business date."""
    hook = get_hook()

    batch_id = hook.get_first(
        f"SELECT {DATABASE}.OPS.SEQ_BATCH_ID.NEXTVAL"
    )[0]

    hook.run(f"""
        INSERT INTO {BATCH_TABLE} (
            BATCH_ID, BUSINESS_DATE, AIRFLOW_DAG_ID,
            AIRFLOW_RUN_ID, STATUS
        )
        VALUES (
            {batch_id},
            TO_DATE({sql_string(business_date)}),
            {sql_string(dag_id)},
            {sql_string(run_id)},
            'RUNNING'
        )
    """)

    return batch_id


def ingest_file(source_key, filename, business_date, batch_id):
    """Idempotently ingest one staged CSV file into its Bronze table."""
    hook = get_hook()

    if source_key not in SOURCES:
        raise ValueError(f"Unknown source: {source_key}")

    source = SOURCES[source_key]
    expected_prefix = source["prefix"]

    if not filename.startswith(expected_prefix + "_"):
        raise ValueError(f"Unexpected filename for {source_key}: {filename}")

    if not re.fullmatch(
        rf"{re.escape(expected_prefix)}_\d{{8}}\.csv", filename
    ):
        raise ValueError(f"Invalid filename: {filename}")

    # Find the exact staged file and retrieve its checksum.
    pattern = f".*{re.escape(filename)}$"
    staged_files = hook.get_records(
        f"LIST @{STAGE} PATTERN={sql_string(pattern)}"
    )

    matching_files = [
        row for row in staged_files
        if PurePosixPath(row[0]).name == filename
    ]

    if not matching_files:
        raise FileNotFoundError(f"{filename} not found in the internal stage")

    stage_path, file_size, file_md5 = matching_files[0][0:3]

    if not file_md5:
        raise RuntimeError(f"Could not retrieve MD5 for {filename}")

    source_name = source["source_name"]
    date_sql = f"TO_DATE({sql_string(business_date)})"
    source_sql = sql_string(source_name)
    md5_sql = sql_string(file_md5)

    # Same file + same checksum already loaded: do nothing.
    existing = hook.get_records(f"""
        SELECT MANIFEST_ID, DROP_SEQ, LOAD_STATUS
        FROM {MANIFEST}
        WHERE SOURCE_NAME = {source_sql}
          AND DROP_DATE = {date_sql}
          AND FILE_MD5 = {md5_sql}
        ORDER BY DROP_SEQ DESC
        LIMIT 1
    """)

    if existing and existing[0][2] == "LOADED":

        hook.run(f"""
            UPDATE {source['table']}
            SET BATCH_ID = {batch_id}
            WHERE DROP_DATE = {date_sql}
            AND DROP_SEQ = {existing[0][1]}
            AND SRC_FILE_NAME = {sql_string(filename)}
        """)

        hook.run(f"""
            UPDATE {MANIFEST}
            SET BATCH_ID = {batch_id}
            WHERE MANIFEST_ID = {existing[0][0]}
        """)

        return {
            "file": filename,
            "status": "SKIPPED_ALREADY_LOADED",
            "rows_loaded": 0,
        }

    if existing:
        # A previous attempt failed or was interrupted: reuse its sequence.
        manifest_id, drop_seq, _ = existing[0]
        hook.run(f"""
            UPDATE {MANIFEST}
            SET LOAD_STATUS = 'LOADING',
                BATCH_ID = {batch_id}
            WHERE MANIFEST_ID = {manifest_id}
        """)
    else:
        # New or changed file: assign the next sequence for this source/date.
        max_seq = hook.get_first(f"""
            SELECT COALESCE(MAX(DROP_SEQ), 0)
            FROM {MANIFEST}
            WHERE SOURCE_NAME = {source_sql}
              AND DROP_DATE = {date_sql}
        """)[0]

        drop_seq = int(max_seq) + 1
        manifest_id = hook.get_first(
            f"SELECT {DATABASE}.BRONZE.SEQ_MANIFEST_ID.NEXTVAL"
        )[0]

        hook.run(f"""
            INSERT INTO {MANIFEST} (
                MANIFEST_ID, SOURCE_NAME, DROP_DATE, DROP_SEQ,
                FILE_NAME, STAGE_TYPE, STAGE_PATH, FILE_MD5,
                ROWS_LOADED, LOAD_STATUS, BATCH_ID
            )
            VALUES (
                {manifest_id}, {source_sql}, {date_sql}, {drop_seq},
                {sql_string(filename)}, 'INTERNAL',
                {sql_string(stage_path)}, {md5_sql},
                0, 'LOADING', {batch_id}
            )
        """)

    # Clear rows from an interrupted attempt using this exact file version.
    # Older drop_seq versions are retained.
    hook.run(f"""
        DELETE FROM {source['table']}
        WHERE DROP_DATE = {date_sql}
          AND DROP_SEQ = {drop_seq}
          AND SRC_FILE_NAME = {sql_string(filename)}
    """)

    positions = source.get(
    "positions",
    list(range(1, len(source["columns"]) + 1)))

    source_select = ", ".join(
        f"${i}" for i in positions
    )
    target_columns = ", ".join(source["columns"])

    # Load source columns plus ingestion metadata.
    copy_sql = f"""
        COPY INTO {source['table']} (
            {target_columns},
            DROP_DATE, DROP_SEQ, SRC_FILE_NAME, SRC_ROW_NUM, BATCH_ID
        )
        FROM (
            SELECT
                {source_select},
                {date_sql},
                {drop_seq},
                METADATA$FILENAME,
                METADATA$FILE_ROW_NUMBER,
                {batch_id}
            FROM @{STAGE}
        )
        FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
        PATTERN = {sql_string(pattern)}
        FORCE = TRUE
        ON_ERROR = 'ABORT_STATEMENT'
    """

    copy_result = hook.get_records(copy_sql)

    # COPY result columns: file, status, rows_parsed, rows_loaded, errors_seen...
    rows_loaded = sum(
        int(row[3] or 0)
        for row in (copy_result or [])
        if len(row) > 3
    )

    hook.run(f"""
        UPDATE {MANIFEST}
        SET ROWS_LOADED = {rows_loaded},
            LOAD_STATUS = 'LOADED',
            BATCH_ID = {batch_id},
            LOADED_TS = CURRENT_TIMESTAMP()
        WHERE MANIFEST_ID = {manifest_id}
    """)

    return {
        "file": filename,
        "status": "LOADED",
        "rows_loaded": rows_loaded,
        "drop_seq": drop_seq,
    }

def update_batch_status(batch_id, status):
    hook = get_hook()

    hook.run(f"""
        UPDATE {BATCH_TABLE}
        SET STATUS = {sql_string(status)},
            ENDED_TS = CURRENT_TIMESTAMP()
        WHERE BATCH_ID = {batch_id}
    """)