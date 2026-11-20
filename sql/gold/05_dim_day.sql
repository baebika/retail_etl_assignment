
-- GOLD: DIM_DAY

MERGE INTO RETAIL_DW.GOLD.DIM_DAY AS target

USING (
    SELECT
        TO_DATE('{BUSINESS_DATE}') AS FULL_DATE,
        DAY(TO_DATE('{BUSINESS_DATE}')) AS "DAY",
        DAYNAME(TO_DATE('{BUSINESS_DATE}')) AS DAY_NAME,
        m.MONTH_KY,
        DAYOFWEEKISO(TO_DATE('{BUSINESS_DATE}')) IN (6, 7)
            AS IS_WEEKEND
    FROM RETAIL_DW.GOLD.DIM_MONTH m
    JOIN RETAIL_DW.GOLD.DIM_QUARTER q
        ON m.QUARTER_KY = q.QUARTER_KY
    WHERE m.MONTH_NAME =
        TO_VARCHAR(TO_DATE('{BUSINESS_DATE}'), 'MMMM')
      AND q."YEAR" = YEAR(TO_DATE('{BUSINESS_DATE}'))
      AND q.QUARTER_NAME =
        'Q' || QUARTER(TO_DATE('{BUSINESS_DATE}'))
) AS source

ON target.FULL_DATE = source.FULL_DATE

WHEN NOT MATCHED THEN
    INSERT (
        DAY_KY,
        FULL_DATE,
        "DAY",
        DAY_NAME,
        MONTH_KY,
        IS_WEEKEND,
        REC_INS_TS
    )
    VALUES (
        RETAIL_DW.GOLD.SEQ_DAY_KY.NEXTVAL,
        source.FULL_DATE,
        source."DAY",
        source.DAY_NAME,
        source.MONTH_KY,
        source.IS_WEEKEND,
        CURRENT_TIMESTAMP()
    );
