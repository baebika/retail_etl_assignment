-- 05_tgt_tables.sql  (Gold)
-- Column names follow the Assignment 2.1 ERD in snake_case. Deviations from the ERD:
--   * tgt_fct_sales     adds txn_id, txn_line_no          (natural key for dedup / idempotency)
--   * tgt_fct_inventory adds eff_from_dt, eff_to_dt, curr_flg (positional history)
--   * Curr_Flag on DIM_Location is standardised to curr_flg
--   * "Day" -> day_num and "Year" -> year_num (avoid date-part keywords as column names)
-- PRIMARY KEY / FOREIGN KEY / UNIQUE are informational in Snowflake (not enforced);
-- the pipeline enforces them with MERGE and NOT EXISTS.
-- SCD behaviour per column is documented in comments (SCD1 / SCD2).
-- Eff_To_DT of the current version is 9999-12-31; a closed version ends the day
-- before its successor's eff_from_dt (inclusive range).

-- ---------------------------------------------------------------- calendar
CREATE TABLE IF NOT EXISTS GOLD.TGT_DIM_QUARTER (
    quarter_ky      INTEGER        NOT NULL,   -- YYYYQ, e.g. 20261
    quarter_name    VARCHAR        NOT NULL,   -- Q1..Q4
    year_num        INTEGER        NOT NULL,
    CONSTRAINT pk_tgt_dim_quarter PRIMARY KEY (quarter_ky)
);

CREATE TABLE IF NOT EXISTS GOLD.TGT_DIM_MONTH (
    month_ky        INTEGER        NOT NULL,   -- YYYYMM, e.g. 202603
    month_name      VARCHAR        NOT NULL,
    quarter_ky      INTEGER        NOT NULL,
    CONSTRAINT pk_tgt_dim_month PRIMARY KEY (month_ky),
    CONSTRAINT fk_tgt_dim_month_quarter FOREIGN KEY (quarter_ky) REFERENCES GOLD.TGT_DIM_QUARTER (quarter_ky)
);

CREATE TABLE IF NOT EXISTS GOLD.TGT_DIM_DAY (
    day_ky          INTEGER        NOT NULL,   -- YYYYMMDD, e.g. 20260304
    full_date       DATE           NOT NULL,
    day_num         INTEGER        NOT NULL,   -- day of month
    day_name        VARCHAR        NOT NULL,
    month_ky        INTEGER        NOT NULL,
    is_weekend      BOOLEAN        NOT NULL,
    rec_ins_ts      TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    rec_updt_ts     TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    CONSTRAINT pk_tgt_dim_day PRIMARY KEY (day_ky),
    CONSTRAINT fk_tgt_dim_day_month FOREIGN KEY (month_ky) REFERENCES GOLD.TGT_DIM_MONTH (month_ky)
);

-- ---------------------------------------------------------------- item hierarchy
-- SCD1 in 2.3: one row per category_id; category_name is overwritten in place.
CREATE TABLE IF NOT EXISTS GOLD.TGT_DIM_CATEGORY (
    category_ky     INTEGER        NOT NULL,
    category_id     VARCHAR        NOT NULL,   -- business key
    category_name   VARCHAR,                   -- SCD1
    curr_flg        BOOLEAN        NOT NULL DEFAULT TRUE,
    eff_from_dt     DATE           NOT NULL,
    eff_to_dt       DATE           NOT NULL DEFAULT DATE '9999-12-31',
    rec_ins_ts      TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    rec_updt_ts     TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    src_sys_cd      INTEGER        NOT NULL,
    batch_id        INTEGER        NOT NULL,
    CONSTRAINT pk_tgt_dim_category PRIMARY KEY (category_ky),
    CONSTRAINT uq_tgt_dim_category_bk UNIQUE (category_id)
);

-- SCD2 on brand + category_ky; SCD1 on item_name (overwritten on EVERY version).
CREATE TABLE IF NOT EXISTS GOLD.TGT_DIM_ITEM (
    item_ky         INTEGER        NOT NULL,
    item_id         VARCHAR        NOT NULL,   -- business key
    item_name       VARCHAR,                   -- SCD1
    brand           VARCHAR,                   -- SCD2
    category_ky     INTEGER        NOT NULL,   -- SCD2 (hierarchy parent)
    curr_flg        BOOLEAN        NOT NULL DEFAULT TRUE,
    eff_from_dt     DATE           NOT NULL,
    eff_to_dt       DATE           NOT NULL DEFAULT DATE '9999-12-31',
    rec_ins_ts      TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    rec_updt_ts     TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    src_sys_cd      INTEGER        NOT NULL,
    batch_id        INTEGER        NOT NULL,
    CONSTRAINT pk_tgt_dim_item PRIMARY KEY (item_ky),
    CONSTRAINT uq_tgt_dim_item_bk_eff UNIQUE (item_id, eff_from_dt),
    CONSTRAINT fk_tgt_dim_item_category FOREIGN KEY (category_ky) REFERENCES GOLD.TGT_DIM_CATEGORY (category_ky)
);

-- ---------------------------------------------------------------- location hierarchy
-- SCD1 in 2.3: one row per district_id; name and province overwritten in place.
CREATE TABLE IF NOT EXISTS GOLD.TGT_DIM_DISTRICT (
    district_ky     INTEGER        NOT NULL,
    district_id     VARCHAR        NOT NULL,   -- business key
    district_name   VARCHAR,                   -- SCD1
    province        VARCHAR,                   -- SCD1
    curr_flg        BOOLEAN        NOT NULL DEFAULT TRUE,
    eff_from_dt     DATE           NOT NULL,
    eff_to_dt       DATE           NOT NULL DEFAULT DATE '9999-12-31',
    rec_ins_ts      TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    rec_updt_ts     TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    src_sys_cd      INTEGER        NOT NULL,
    batch_id        INTEGER        NOT NULL,
    CONSTRAINT pk_tgt_dim_district PRIMARY KEY (district_ky),
    CONSTRAINT uq_tgt_dim_district_bk UNIQUE (district_id)
);

-- SCD2 on city + district_ky; SCD1 on location_name (overwritten on EVERY version).
CREATE TABLE IF NOT EXISTS GOLD.TGT_DIM_LOCATION (
    location_ky     INTEGER        NOT NULL,
    location_id     VARCHAR        NOT NULL,   -- business key (store_id in the file)
    location_name   VARCHAR,                   -- SCD1
    city            VARCHAR,                   -- SCD2
    district_ky     INTEGER        NOT NULL,   -- SCD2 (hierarchy parent)
    curr_flg        BOOLEAN        NOT NULL DEFAULT TRUE,
    eff_from_dt     DATE           NOT NULL,
    eff_to_dt       DATE           NOT NULL DEFAULT DATE '9999-12-31',
    rec_ins_ts      TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    rec_updt_ts     TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    src_sys_cd      INTEGER        NOT NULL,
    batch_id        INTEGER        NOT NULL,
    CONSTRAINT pk_tgt_dim_location PRIMARY KEY (location_ky),
    CONSTRAINT uq_tgt_dim_location_bk_eff UNIQUE (location_id, eff_from_dt),
    CONSTRAINT fk_tgt_dim_location_district FOREIGN KEY (district_ky) REFERENCES GOLD.TGT_DIM_DISTRICT (district_ky)
);

-- ---------------------------------------------------------------- customer
-- No customer source file in 2.3: holds only the seeded Unknown member (customer_ky = -1).
CREATE TABLE IF NOT EXISTS GOLD.TGT_DIM_CUSTOMER (
    customer_ky     INTEGER        NOT NULL,
    customer_id     VARCHAR        NOT NULL,
    customer_name   VARCHAR,
    gender          VARCHAR,
    contact         VARCHAR,
    rec_ins_ts      TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    rec_updt_ts     TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    src_sys_cd      INTEGER        NOT NULL,
    batch_id        INTEGER        NOT NULL,
    CONSTRAINT pk_tgt_dim_customer PRIMARY KEY (customer_ky)
);

-- ---------------------------------------------------------------- facts
-- Insert-only. Natural key (txn_id, txn_line_no); loaded with NOT EXISTS protection.
CREATE TABLE IF NOT EXISTS GOLD.TGT_FCT_SALES (
    sales_id            INTEGER        NOT NULL,
    txn_id              VARCHAR        NOT NULL,
    txn_line_no         INTEGER        NOT NULL,
    item_ky             INTEGER        NOT NULL,   -- item version effective on the sales date
    location_ky         INTEGER        NOT NULL,   -- location version effective on the sales date
    customer_ky         INTEGER        NOT NULL,   -- always -1 (Unknown) in 2.3
    day_ky              INTEGER        NOT NULL,
    quantity_sold       INTEGER        NOT NULL,
    unit_selling_price  NUMBER(12,2)   NOT NULL,
    discount_amt        NUMBER(12,2)   NOT NULL,
    total_sales_amt     NUMBER(14,2)   NOT NULL,
    rec_ins_ts          TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    rec_updt_ts         TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    src_sys_cd          INTEGER        NOT NULL,
    batch_id            INTEGER        NOT NULL,
    CONSTRAINT pk_tgt_fct_sales PRIMARY KEY (sales_id),
    CONSTRAINT uq_tgt_fct_sales_nk UNIQUE (txn_id, txn_line_no),
    CONSTRAINT fk_tgt_fct_sales_item FOREIGN KEY (item_ky) REFERENCES GOLD.TGT_DIM_ITEM (item_ky),
    CONSTRAINT fk_tgt_fct_sales_location FOREIGN KEY (location_ky) REFERENCES GOLD.TGT_DIM_LOCATION (location_ky),
    CONSTRAINT fk_tgt_fct_sales_customer FOREIGN KEY (customer_ky) REFERENCES GOLD.TGT_DIM_CUSTOMER (customer_ky),
    CONSTRAINT fk_tgt_fct_sales_day FOREIGN KEY (day_ky) REFERENCES GOLD.TGT_DIM_DAY (day_ky)
);

-- Positional: one row = one quantity held for one item/location over [eff_from_dt, eff_to_dt].
-- day_ky is the day the position was opened (= eff_from_dt as YYYYMMDD).
-- Changed quantity -> close the old row (curr_flg = FALSE) and insert a new one; never delete.
CREATE TABLE IF NOT EXISTS GOLD.TGT_FCT_INVENTORY (
    inventory_id        INTEGER        NOT NULL,
    item_ky             INTEGER        NOT NULL,
    location_ky         INTEGER        NOT NULL,
    day_ky              INTEGER        NOT NULL,
    quantity_on_hand    INTEGER        NOT NULL,
    eff_from_dt         DATE           NOT NULL,
    eff_to_dt           DATE           NOT NULL DEFAULT DATE '9999-12-31',
    curr_flg            BOOLEAN        NOT NULL DEFAULT TRUE,
    rec_ins_ts          TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    rec_updt_ts         TIMESTAMP_NTZ  NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    src_sys_cd          INTEGER        NOT NULL,
    batch_id            INTEGER        NOT NULL,
    CONSTRAINT pk_tgt_fct_inventory PRIMARY KEY (inventory_id),
    CONSTRAINT uq_tgt_fct_inventory_pos UNIQUE (item_ky, location_ky, eff_from_dt),
    CONSTRAINT fk_tgt_fct_inventory_item FOREIGN KEY (item_ky) REFERENCES GOLD.TGT_DIM_ITEM (item_ky),
    CONSTRAINT fk_tgt_fct_inventory_location FOREIGN KEY (location_ky) REFERENCES GOLD.TGT_DIM_LOCATION (location_ky),
    CONSTRAINT fk_tgt_fct_inventory_day FOREIGN KEY (day_ky) REFERENCES GOLD.TGT_DIM_DAY (day_ky)
);
