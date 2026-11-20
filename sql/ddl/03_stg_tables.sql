-- 03_stg_tables.sql  (Bronze)
-- Raw text mirror of each CSV: every file column is VARCHAR with the same name as the
-- file header, followed by the same six metadata columns on every table.
-- The original file bytes stay untouched in the stage; these tables are never edited
-- except to reload the SAME drop_date + drop_seq after a failed load.

CREATE TABLE IF NOT EXISTS BRONZE.STG_ITEM_MASTER (
    item_id         VARCHAR,
    item_name       VARCHAR,
    brand           VARCHAR,
    category_id     VARCHAR,
    category_name   VARCHAR,
    drop_date       DATE           NOT NULL,
    drop_seq        INTEGER        NOT NULL,
    src_file_name   VARCHAR        NOT NULL,
    src_row_num     INTEGER        NOT NULL,
    batch_id        INTEGER        NOT NULL,
    load_ts         TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS BRONZE.STG_STORE_MASTER (
    store_id        VARCHAR,
    store_name      VARCHAR,
    city            VARCHAR,
    district_id     VARCHAR,
    district_name   VARCHAR,
    province        VARCHAR,
    drop_date       DATE           NOT NULL,
    drop_seq        INTEGER        NOT NULL,
    src_file_name   VARCHAR        NOT NULL,
    src_row_num     INTEGER        NOT NULL,
    batch_id        INTEGER        NOT NULL,
    load_ts         TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS BRONZE.STG_POS_SALES (
    txn_id          VARCHAR,
    line_no         VARCHAR,
    txn_ts          VARCHAR,
    store_id        VARCHAR,
    item_id         VARCHAR,
    quantity        VARCHAR,
    unit_price      VARCHAR,
    discount_amt    VARCHAR,
    drop_date       DATE           NOT NULL,
    drop_seq        INTEGER        NOT NULL,
    src_file_name   VARCHAR        NOT NULL,
    src_row_num     INTEGER        NOT NULL,
    batch_id        INTEGER        NOT NULL,
    load_ts         TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS BRONZE.STG_INVENTORY_SNAPSHOT (
    snapshot_date   VARCHAR,
    store_id        VARCHAR,
    item_id         VARCHAR,
    qty_on_hand     VARCHAR,
    drop_date       DATE           NOT NULL,
    drop_seq        INTEGER        NOT NULL,
    src_file_name   VARCHAR        NOT NULL,
    src_row_num     INTEGER        NOT NULL,
    batch_id        INTEGER        NOT NULL,
    load_ts         TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

-- One row per file landed (G1). Drives idempotent ingestion:
-- same drop_date + same file_md5 already LOADED -> skip; different md5 -> next drop_seq.
CREATE TABLE IF NOT EXISTS BRONZE.STG_OPS_INGEST_MANIFEST (
    manifest_id     INTEGER        NOT NULL,
    source_name     VARCHAR        NOT NULL,   -- ITEM_MASTER | STORE_MASTER | POS_SALES | INVENTORY_SNAPSHOT
    drop_date       DATE           NOT NULL,
    drop_seq        INTEGER        NOT NULL,
    file_name       VARCHAR        NOT NULL,
    stage_type      VARCHAR        NOT NULL,   -- INTERNAL | EXTERNAL
    stage_path      VARCHAR        NOT NULL,
    file_md5        VARCHAR        NOT NULL,
    rows_loaded     INTEGER,
    load_status     VARCHAR        NOT NULL,   -- LOADED | SKIPPED_DUPLICATE | FAILED
    batch_id        INTEGER        NOT NULL,
    loaded_ts       TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_stg_ops_ingest_manifest PRIMARY KEY (manifest_id)
);
