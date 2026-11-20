import pendulum
from pathlib import Path
from datetime import timedelta

from airflow.sdk import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from ingestion import create_batch, ingest_file, SOURCES, update_batch_status


@task
def create_batch_task(business_date, dag_id, run_id):
    return create_batch(business_date, dag_id, run_id)


@task
def build_file_specs(business_date):
    file_date = business_date.replace("-", "")

    return [
        {
            "source_key": key,
            "filename": f"{source['prefix']}_{file_date}.csv",
        }
        for key, source in SOURCES.items()
    ]


@task
def ingest_one_file(source_key, filename, business_date, batch_id):
    return ingest_file(
        source_key=source_key,
        filename=filename,
        business_date=business_date,
        batch_id=batch_id,
    )

def mark_batch_failed(context):
    ti = context["ti"]

    batch_id = ti.xcom_pull(task_ids="create_batch_task")

    if batch_id:
        update_batch_status(batch_id, "FAILED")

@dag(
    dag_id="retail_etl",
    start_date=pendulum.datetime(2026, 11, 10, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["retail", "assignment2.3"],
    on_failure_callback=mark_batch_failed,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    }
)
def retail_etl():

    from airflow.sdk import get_current_context

    @task(multiple_outputs=True)
    def get_run_details():
        context = get_current_context()
        return {
            "business_date": context["ds"],
            "dag_id": context["dag"].dag_id,
            "run_id": context["run_id"],
        }

    @task
    def silver_transform(business_date, batch_id):

        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        sql_dir = Path(__file__).parent.parent / "sql" / "silver"

        sql_files = [
            "01_silver_category.sql",
            "02_silver_district.sql",
            "03_silver_item.sql",
            "04_silver_location.sql",
            "05_silver_quarantine.sql",
            "06_silver_sales.sql",
            "07_silver_inventory.sql",
        ]

        for filename in sql_files:
            sql = (sql_dir / filename).read_text(encoding="utf-8")

            sql = sql.format(
                BUSINESS_DATE=business_date,
                BATCH_ID=batch_id,
            )

            hook.run(sql, split_statements=True)

        return f"Silver transformation completed for {business_date}"


    @task
    def validate_silver_integrity(business_date, batch_id):

        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        checks = {
            "DUPLICATE_CATEGORY": """
                SELECT COUNT(*) FROM (
                    SELECT CATEGORY_ID
                    FROM RETAIL_DW.SILVER.TMP_CATEGORY
                    WHERE DROP_DATE = TO_DATE('{BUSINESS_DATE}')
                    AND BATCH_ID = {BATCH_ID}
                    GROUP BY CATEGORY_ID
                    HAVING COUNT(*) > 1
                )
            """,

            "DUPLICATE_DISTRICT": """
                SELECT COUNT(*) FROM (
                    SELECT DISTRICT_ID
                    FROM RETAIL_DW.SILVER.TMP_DISTRICT
                    WHERE DROP_DATE = TO_DATE('{BUSINESS_DATE}')
                    AND BATCH_ID = {BATCH_ID}
                    GROUP BY DISTRICT_ID
                    HAVING COUNT(*) > 1
                )
            """,

            "DUPLICATE_ITEM": """
                SELECT COUNT(*) FROM (
                    SELECT ITEM_ID
                    FROM RETAIL_DW.SILVER.TMP_ITEM
                    WHERE DROP_DATE = TO_DATE('{BUSINESS_DATE}')
                    AND BATCH_ID = {BATCH_ID}
                    GROUP BY ITEM_ID
                    HAVING COUNT(*) > 1
                )
            """,

            "DUPLICATE_LOCATION": """
                SELECT COUNT(*) FROM (
                    SELECT LOCATION_ID
                    FROM RETAIL_DW.SILVER.TMP_LOCATION
                    WHERE DROP_DATE = TO_DATE('{BUSINESS_DATE}')
                    AND BATCH_ID = {BATCH_ID}
                    GROUP BY LOCATION_ID
                    HAVING COUNT(*) > 1
                )
            """,

            "ORPHAN_ITEM_CATEGORY": """
                SELECT COUNT(*)
                FROM RETAIL_DW.SILVER.TMP_ITEM i
                LEFT JOIN RETAIL_DW.SILVER.TMP_CATEGORY c
                    ON i.CATEGORY_ID = c.CATEGORY_ID
                    AND c.DROP_DATE = TO_DATE('{BUSINESS_DATE}')
                    AND c.BATCH_ID = {BATCH_ID}
                WHERE i.DROP_DATE = TO_DATE('{BUSINESS_DATE}')
                AND i.BATCH_ID = {BATCH_ID}
                AND c.CATEGORY_ID IS NULL
            """,

            "ORPHAN_LOCATION_DISTRICT": """
                SELECT COUNT(*)
                FROM RETAIL_DW.SILVER.TMP_LOCATION l
                LEFT JOIN RETAIL_DW.SILVER.TMP_DISTRICT d
                    ON l.DISTRICT_ID = d.DISTRICT_ID
                    AND d.DROP_DATE = TO_DATE('{BUSINESS_DATE}')
                    AND d.BATCH_ID = {BATCH_ID}
                WHERE l.DROP_DATE = TO_DATE('{BUSINESS_DATE}')
                AND l.BATCH_ID = {BATCH_ID}
                AND d.DISTRICT_ID IS NULL
            """
        }

        failures = []

        for check_name, query in checks.items():

            sql = query.format(
                BUSINESS_DATE=business_date,
                BATCH_ID=batch_id
            )

            issue_count = hook.get_first(sql)[0]

            print(f"{check_name}: {issue_count} issues")

            if issue_count > 0:
                failures.append(f"{check_name}: {issue_count} issues")

        if failures:
            raise ValueError(
                "Silver integrity validation failed: "
                + "; ".join(failures)
            )

        return "Silver integrity validation passed"

    @task
    def gold_transform(business_date, batch_id):

        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        sql_dir = Path(__file__).parent.parent / "sql" / "gold"

        sql_files = [
            "01_dim_category.sql",
            "02_dim_district.sql",
            "03_dim_quarter.sql",
            "04_dim_month.sql",
            "05_dim_day.sql",
            "06_dim_item.sql",
            "07_dim_location.sql",
            "09_fct_sales.sql",
            "10_fct_inventory.sql",
            "11_aggregate_sales.sql",
        ]

        for filename in sql_files:
            sql = (sql_dir / filename).read_text(encoding="utf-8")

            sql = sql.format(
                BUSINESS_DATE=business_date,
                BATCH_ID=batch_id,
            )

            hook.run(sql, split_statements=True)

        return f"Gold transformation completed for {business_date}"

    
    
    @task
    def reconcile_data(business_date, batch_id):

        hook = SnowflakeHook(snowflake_conn_id="snowflake_default")

        date = f"TO_DATE('{business_date}')"

        queries = {
            "POS_ROW_COUNT": """
                SELECT
                    (SELECT COUNT(*)
                    FROM RETAIL_DW.BRONZE.STG_POS_SALES
                    WHERE DROP_DATE = {date}
                    AND BATCH_ID = {batch_id}),

                    (SELECT COUNT(*)
                    FROM RETAIL_DW.SILVER.TMP_SALES_LINE
                    WHERE DROP_DATE = {date}
                    AND BATCH_ID = {batch_id})
                    +
                    (SELECT COUNT(*)
                    FROM RETAIL_DW.SILVER.TMP_QUARANTINE_ROW
                    WHERE DROP_DATE = {date}
                    AND SOURCE_NAME = 'POS_SALES'
                    AND BATCH_ID = {batch_id})
            """,

            "SALES_ROW_COUNT": """
                SELECT
                    (SELECT COUNT(*)
                    FROM RETAIL_DW.SILVER.TMP_SALES_LINE
                    WHERE DROP_DATE = {date}
                    AND BATCH_ID = {batch_id}),

                    (SELECT COUNT(*)
                    FROM RETAIL_DW.GOLD.FCT_SALES f
                    JOIN RETAIL_DW.GOLD.DIM_DAY d
                    ON f.DAY_KY = d.DAY_KY
                    WHERE d.FULL_DATE = {date})
            """,

            "SALES_NET_AMOUNT": """
                SELECT
                    (SELECT COALESCE(SUM(TOTAL_SALES_AMT), 0)
                    FROM RETAIL_DW.SILVER.TMP_SALES_LINE
                    WHERE DROP_DATE = {date}
                    AND BATCH_ID = {batch_id}),

                    (SELECT COALESCE(SUM(f.TOTAL_SALES_AMT), 0)
                    FROM RETAIL_DW.GOLD.FCT_SALES f
                    JOIN RETAIL_DW.GOLD.DIM_DAY d
                    ON f.DAY_KY = d.DAY_KY
                    WHERE d.FULL_DATE = {date})
            """,

            "INVENTORY_BRONZE_SILVER": """
                SELECT
                    (SELECT COUNT(*)
                    FROM RETAIL_DW.BRONZE.STG_INVENTORY_SNAPSHOT
                    WHERE DROP_DATE = {date}
                    AND BATCH_ID = {batch_id}),

                    (SELECT COUNT(*)
                    FROM RETAIL_DW.SILVER.TMP_INVENTORY_SNAPSHOT
                    WHERE DROP_DATE = {date}
                    AND BATCH_ID = {batch_id})
            """,

            "INVENTORY_SILVER_GOLD": """
                SELECT
                    (SELECT COUNT(*)
                    FROM RETAIL_DW.SILVER.TMP_INVENTORY_SNAPSHOT
                    WHERE DROP_DATE = {date}
                    AND BATCH_ID = {batch_id}),

                    (SELECT COUNT(*)
                    FROM RETAIL_DW.GOLD.FCT_INVENTORY f
                    JOIN RETAIL_DW.GOLD.DIM_DAY d
                    ON f.DAY_KY = d.DAY_KY
                    WHERE d.FULL_DATE = {date})
            """,

            # DIM_ITEM SCD2 checks
            "ITEM_DUPLICATE_CURRENT": """
                SELECT 0,
                    COUNT(*)
                FROM (
                    SELECT ITEM_ID
                    FROM RETAIL_DW.GOLD.DIM_ITEM
                    WHERE CURR_FLG = TRUE
                    GROUP BY ITEM_ID
                    HAVING COUNT(*) > 1
                )
            """,

            "ITEM_INVALID_DATES": """
                SELECT 0,
                    COUNT(*)
                FROM RETAIL_DW.GOLD.DIM_ITEM
                WHERE EFF_TO_DT < EFF_FROM_DT
            """,

            # DIM_LOCATION SCD2 checks
            "LOCATION_DUPLICATE_CURRENT": """
                SELECT 0,
                    COUNT(*)
                FROM (
                    SELECT LOCATION_ID
                    FROM RETAIL_DW.GOLD.DIM_LOCATION
                    WHERE CURR_FLG = TRUE
                    GROUP BY LOCATION_ID
                    HAVING COUNT(*) > 1
                )
            """,

            "LOCATION_INVALID_DATES": """
                SELECT 0,
                    COUNT(*)
                FROM RETAIL_DW.GOLD.DIM_LOCATION
                WHERE EFF_TO_DT < EFF_FROM_DT
            """,

            # Positional inventory closure check
            "INVENTORY_POSITION_CLOSURE": """
                SELECT 0,
                    COUNT(*)
                FROM RETAIL_DW.GOLD.FCT_INVENTORY f
                JOIN RETAIL_DW.GOLD.DIM_ITEM i
                    ON f.ITEM_KY = i.ITEM_KY
                JOIN RETAIL_DW.GOLD.DIM_LOCATION l
                    ON f.LOCATION_KY = l.LOCATION_KY
                WHERE f.CURR_FLG = TRUE
                  AND f.EFF_TO_DT = '9999-12-31'
                  AND NOT EXISTS (
                      SELECT 1
                      FROM RETAIL_DW.SILVER.TMP_INVENTORY_SNAPSHOT s
                      WHERE s.DROP_DATE = {date}
                        AND s.BATCH_ID = {batch_id}
                        AND s.ITEM_ID = i.ITEM_ID
                        AND s.LOCATION_ID = l.LOCATION_ID
                  )
            """
        }

        failures = []

        for check_code, query in queries.items():

            sql = query.format(
                date=date,
                batch_id=batch_id
            )

            expected, actual = hook.get_first(sql)

            status = (
                "PASSED"
                if round(float(expected or 0), 2)
                == round(float(actual or 0), 2)
                else "FAILED"
            )

            hook.run(f"""
                INSERT INTO RETAIL_DW.OPS.OPS_RECON_RESULT (
                    RECON_ID,
                    BATCH_ID,
                    BUSINESS_DATE,
                    CHECK_CODE,
                    CHECK_NAME,
                    EXPECTED_VALUE,
                    ACTUAL_VALUE,
                    STATUS,
                    CHECKED_TS
                )
                VALUES (
                    RETAIL_DW.OPS.SEQ_RECON_ID.NEXTVAL,
                    {batch_id},
                    {date},
                    '{check_code}',
                    '{check_code}',
                    {expected or 0},
                    {actual or 0},
                    '{status}',
                    CURRENT_TIMESTAMP()
                )
            """)

            print(
                f"{check_code}: expected={expected}, "
                f"actual={actual}, status={status}"
            )

            if status == "FAILED":
                failures.append(check_code)

        if failures:
            raise ValueError(
                "Reconciliation failed: " + ", ".join(failures)
            )

        return "All reconciliation checks passed"


    @task
    def mark_batch_success(batch_id):
        update_batch_status(batch_id, "SUCCESS")
    

    details = get_run_details()

    batch_id = create_batch_task(
    business_date=details["business_date"],
    dag_id=details["dag_id"],
    run_id=details["run_id"],
    )

    files = build_file_specs(details["business_date"])

    ingestion = ingest_one_file.partial(
        business_date=details["business_date"],
        batch_id=batch_id,
    ).expand_kwargs(files)

    silver = silver_transform(
        business_date=details["business_date"],
        batch_id=batch_id,
    )
    
    silver_validation = validate_silver_integrity(
        business_date=details["business_date"],
        batch_id=batch_id,
    )

    gold = gold_transform(
        business_date=details["business_date"],
        batch_id=batch_id,
    )

    reconciliation = reconcile_data(
        business_date=details["business_date"],
        batch_id=batch_id,
    )

    batch_success = mark_batch_success(batch_id)

    # Task dependencies
    ingestion >> silver
    silver >> silver_validation >> gold
    gold >> reconciliation >> batch_success


retail_etl()

