{% snapshot snp_dim_cust_act %}

{{
    config(
      target_schema='snapshots',
      unique_key='ACT_NO',
      strategy='check',
      updated_at='TABLE_DATE',
      check_cols=['ID', 'ID_DUP_NO', 'CUST_UNIQ_ID', 'STATUS_PBA_CODE'],
      invalidate_hard_deletes=True,
      tags=["snapshot", "monthly_job"]
    )
}}

SELECT
    ACT_NO,
    TABLE_DATE,
    CONVERT(VARCHAR(64), 
        HASHBYTES(
            'SHA2_256', 
            CONCAT(ISNULL(ID, ''), '|', ISNULL(ID_DUP_NO, ''))
        ), 
        2
    ) AS CUST_UNIQ_ID,
    ID,
    ID_DUP_NO,
    STATUS_PBA_CODE,
    PBA_INT_CODE
FROM {{ source('database', 'T_CUST_PS') }}

{% endsnapshot %}