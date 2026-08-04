-- dbt config
{{
    config(
        materialized='incremental',
        unique_key='Seq_ID',
        tags=['daily_job'],
        incremental_strategy='delete+insert', 
        alias='T_TXN_PS_NET'
    )
}}

{% set target_date = var('target_date') %}

WITH reversal_candidates AS (
    SELECT
        A.Seq_ID
        , A.CPU_DATE
        , A.FUNC_CODE
        , A.ACT_NO
        , A.STATUS_CODE_PBA
        , A.TXN_AMT
        , A.TXN_TYPE
        , A.CD_FLG
        , A.DR_FLG
        , A.CHANNEL_FLG
        , A.WITH_BOOK_FLG
        , A.TXN_TIME
        , A.PS_BAL
        , A.TXN_BRH
        , A.EMP_NO
        , A.JRNL_NO
        , A.TXN_NAME
        , A.TXN_MEMO
        , A.ACCT_NBR_ORI
        , A.DRCR
        , A.OWN_TRANS_ACCT
        , A.CUR
        , A.BNK_ID
        , A.TXN_MEMO_C
        , A.TXN_ENAME
        , A.IS_102_ACT
        , A.IS_GIRO_ACT_FLG
        , A.TABLE_DATE
        , A.BATCH_ID
        , CAST(
            CONVERT(VARCHAR(10), A.CPU_DATE, 120) + ' ' +
            STUFF(STUFF(A.TXN_TIME, 5, 0, ':'), 3, 0, ':')
            AS DATETIME2
        ) AS A_DATETIME
    FROM {{ source('database', 'T_TXN_PS') }} AS A
    WHERE A.TABLE_DATE = '{{ target_date }}'
      AND A.TXN_NAME LIKE '%沖%'
),

matched_pairs AS (
    SELECT
        RC.Seq_ID AS A_SEQ_ID
        , B.Seq_ID  AS B_SEQ_ID
        , ROW_NUMBER() OVER (
            PARTITION BY RC.Seq_ID
            ORDER BY
                DATEDIFF(SECOND,
                    CAST(
                        CONVERT(VARCHAR(10), B.CPU_DATE, 120) + ' ' +
                        STUFF(STUFF(B.TXN_TIME, 5, 0, ':'), 3, 0, ':')
                        AS DATETIME2
                    ),
                    RC.A_DATETIME
                ) ASC,
                ( CASE WHEN RC.TXN_NAME    = B.TXN_NAME    THEN 1 ELSE 0 END
                + CASE WHEN RC.CHANNEL_FLG = B.CHANNEL_FLG THEN 1 ELSE 0 END
                + CASE WHEN RC.FUNC_CODE   = B.FUNC_CODE   THEN 1 ELSE 0 END
                + CASE WHEN RC.CD_FLG      = B.CD_FLG      THEN 1 ELSE 0 END
                ) DESC
        ) AS RN
    FROM reversal_candidates AS RC
    JOIN {{ source('database', 'T_TXN_PS') }} AS B
        ON  B.ACT_NO  = RC.ACT_NO
        AND B.TXN_AMT = RC.TXN_AMT
        AND B.CUR     = RC.CUR
        AND B.DR_FLG  != RC.DR_FLG
        AND B.Seq_ID  != RC.Seq_ID
        AND B.TABLE_DATE BETWEEN DATEADD(DAY, -3, '{{ target_date }}') AND '{{ target_date }}'
    WHERE
        CAST(
            CONVERT(VARCHAR(10), B.CPU_DATE, 120) + ' ' +
            STUFF(STUFF(B.TXN_TIME, 5, 0, ':'), 3, 0, ':')
            AS DATETIME2
        ) <= RC.A_DATETIME
),

best_matches AS (
    SELECT * FROM matched_pairs WHERE RN = 1
),

removed_seq_ids AS (
    SELECT Seq_ID FROM reversal_candidates
    UNION
    SELECT B_SEQ_ID AS Seq_ID FROM best_matches
)

SELECT
    A.Seq_ID
    , A.CPU_DATE
    , A.FUNC_CODE
    , A.ACT_NO
    , A.STATUS_CODE_PBA
    , A.TXN_AMT
    , A.TXN_TYPE
    , A.CD_FLG
    , A.DR_FLG
    , A.CHANNEL_FLG
    , A.WITH_BOOK_FLG
    , A.TXN_TIME
    , A.PS_BAL
    , A.TXN_BRH
    , A.EMP_NO
    , A.JRNL_NO
    , A.TXN_NAME
    , A.TXN_MEMO
    , A.ACCT_NBR_ORI
    , A.DRCR
    , A.OWN_TRANS_ACCT
    , A.CUR
    , A.BNK_ID
    , A.TXN_MEMO_C
    , A.TXN_ENAME
    , A.IS_102_ACT
    , A.IS_GIRO_ACT_FLG
    , A.TABLE_DATE
    , A.BATCH_ID
FROM {{ source('database', 'T_TXN_PS') }} AS A
WHERE NOT EXISTS (
    SELECT 1 FROM removed_seq_ids R WHERE R.Seq_ID = A.Seq_ID
)

{% if is_incremental() %}
  AND A.TABLE_DATE = '{{ target_date }}'
{% endif %}