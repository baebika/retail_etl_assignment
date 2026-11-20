-- 06_ops_tables.sql
-- Pipeline bookkeeping. OPS_ETL_BATCH is the source of every Batch_ID;
-- OPS_RECON_RESULT stores each reconciliation check so a failed DAG can be explained.

CREATE TABLE IF NOT EXISTS OPS.OPS_ETL_BATCH (
    batch_id        INTEGER        NOT NULL,
    business_date   DATE           NOT NULL,
    airflow_dag_id  VARCHAR        NOT NULL,
    airflow_run_id  VARCHAR        NOT NULL,
    status          VARCHAR        NOT NULL,   -- RUNNING | SUCCESS | FAILED
    started_ts      TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    ended_ts        TIMESTAMP_NTZ,
    CONSTRAINT pk_ops_etl_batch PRIMARY KEY (batch_id)
);

CREATE TABLE IF NOT EXISTS OPS.OPS_RECON_RESULT (
    recon_id        INTEGER        NOT NULL,
    batch_id        INTEGER        NOT NULL,
    business_date   DATE           NOT NULL,
    check_code      VARCHAR        NOT NULL,   -- R01..R13 from config/quality_rules.yaml
    check_name      VARCHAR        NOT NULL,
    expected_value  NUMBER(20,2),
    actual_value    NUMBER(20,2),
    status          VARCHAR        NOT NULL,   -- PASS | FAIL | ALERT
    checked_ts      TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_ops_recon_result PRIMARY KEY (recon_id)
);
