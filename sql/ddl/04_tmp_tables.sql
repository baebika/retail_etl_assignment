-- 04_tmp_tables.sql  (Silver)
-- Typed and validated rows. Every table is rebuilt per drop_date
-- (DELETE WHERE drop_date = D, then INSERT, in one transaction) so re-runs are idempotent.
-- These are permanent tables; session-scoped Snowflake TEMPORARY tables used inside
-- tasks are named wrk_* so the two are never confused.

CREATE TABLE IF NOT EXISTS SILVER.TMP_ITEM (
    item_id         VARCHAR        NOT NULL,
    item_name       VARCHAR,
    brand           VARCHAR,
    category_id     VARCHAR        NOT NULL,
    drop_date       DATE           NOT NULL,
    src_file_name   VARCHAR        NOT NULL,
    src_row_num     INTEGER        NOT NULL,
    batch_id        INTEGER        NOT NULL,
    rec_ins_ts      TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SILVER.TMP_CATEGORY (
    category_id     VARCHAR        NOT NULL,
    category_name   VARCHAR,
    drop_date       DATE           NOT NULL,
    batch_id        INTEGER        NOT NULL,
    rec_ins_ts      TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SILVER.TMP_LOCATION (
    location_id     VARCHAR        NOT NULL,   -- = store_id in the file
    location_name   VARCHAR,
    city            VARCHAR,
    district_id     VARCHAR        NOT NULL,
    drop_date       DATE           NOT NULL,
    src_file_name   VARCHAR        NOT NULL,
    src_row_num     INTEGER        NOT NULL,
    batch_id        INTEGER        NOT NULL,
    rec_ins_ts      TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SILVER.TMP_DISTRICT (
    district_id     VARCHAR        NOT NULL,
    district_name   VARCHAR,
    province        VARCHAR,
    drop_date       DATE           NOT NULL,
    batch_id        INTEGER        NOT NULL,
    rec_ins_ts      TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SILVER.TMP_SALES_LINE (
    txn_id              VARCHAR        NOT NULL,
    txn_line_no         INTEGER        NOT NULL,
    sales_ts            TIMESTAMP_NTZ  NOT NULL,
    sales_date          DATE           NOT NULL,
    location_id         VARCHAR        NOT NULL,
    item_id             VARCHAR        NOT NULL,
    quantity_sold       INTEGER        NOT NULL,
    unit_selling_price  NUMBER(12,2)   NOT NULL,
    discount_amt        NUMBER(12,2)   NOT NULL,
    total_sales_amt     NUMBER(14,2)   NOT NULL,   -- quantity_sold * unit_selling_price - discount_amt
    drop_date           DATE           NOT NULL,
    src_file_name       VARCHAR        NOT NULL,
    src_row_num         INTEGER        NOT NULL,
    batch_id            INTEGER        NOT NULL,
    rec_ins_ts          TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SILVER.TMP_INVENTORY_SNAPSHOT (
    snapshot_date       DATE           NOT NULL,
    location_id         VARCHAR        NOT NULL,
    item_id             VARCHAR        NOT NULL,
    quantity_on_hand    INTEGER        NOT NULL,
    drop_date           DATE           NOT NULL,
    src_file_name       VARCHAR        NOT NULL,
    src_row_num         INTEGER        NOT NULL,
    batch_id            INTEGER        NOT NULL,
    rec_ins_ts          TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SILVER.TMP_QUARANTINE_ROW (
    quarantine_id   INTEGER        NOT NULL,
    drop_date       DATE           NOT NULL,
    source_name     VARCHAR        NOT NULL,   -- ITEM_MASTER | STORE_MASTER | POS_SALES | INVENTORY_SNAPSHOT
    gate_code       VARCHAR        NOT NULL,   -- Q-code from the 2.2 gate catalog
    reason_code     VARCHAR        NOT NULL,   -- see config/quality_rules.yaml
    reason_desc     VARCHAR,
    src_file_name   VARCHAR        NOT NULL,
    src_row_num     INTEGER        NOT NULL,
    raw_row         VARIANT        NOT NULL,   -- the rejected Bronze row as a JSON object
    batch_id        INTEGER        NOT NULL,
    rec_ins_ts      TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_tmp_quarantine_row PRIMARY KEY (quarantine_id)
);
