/*
Created: 2026-05-08
Description: 
Change Log:
- 2026-05-08 [000000] Create snapshot of T_CUST
*/

{% snapshot snp_cust %}

{{
    config(
      target_schema='snapshots',
      unique_key='CUST_PK',
      strategy='check',
      updated_at='TABLE_DATE',
      check_cols=[
          'ID_TYPE', 'CGT_FLG', 'OCUP_SUB_CODE', 'OCUP_CODE', 'BIRTH_DATE', 
          'ADR', 'PSW_FLG_VC', 'LIVE183_FLG', 'CUI_FLG', 'TEL_AREA_CODE', 
          'ZIP_CODE', 'SEX_CODE', 'TEL_AREA_CODE2', 'HAND_TEL', 'EMAIL_FLG', 
          'NATION_CODE', 'RESIDENT_EXP_DATE', 'DATA_SELL_FLG', 'STOP_USE_FLG', 
          'ID_HEAD', 'HIGH_RISK_FLG', 'MOJ_LAUN_FLG', 'LAST_LAUN_DATE', 'SPC_CNTL_FLG', 
          'POLITICIAN_TYPE_BIT_1', 'POLITICIAN_TYPE_BIT_2', 'POLITICIAN_TYPE_BIT_3', 
          'POLITICIAN_TYPE_BIT_4', 'POLITICIAN_TYPE_BIT_5', 'POLITICIAN_TYPE_BIT_6', 
          'POLITICIAN_TYPE_BIT_7', 'POLITICIAN_TYPE_BIT_8', 'POLITICIAN_TYPE_BIT_9', 
          'POLITICIAN_TYPE_BIT_10', 'POLITICIAN_TYPE_BIT_11', 'POLITICIAN_TYPE_BIT_12', 
          'POLITICIAN_TYPE_BIT_13', 'POLITICIAN_TYPE_BIT_14', 'POLITICIAN_TYPE_BIT_15', 
          'POLITICIAN_TYPE_BIT_16', 'RESIDENT_CITY_DIST', 'AUM_AMT', 'EMAIL', 'NAME', 
          'REL_1_ID', 'REL_1_TITLE', 'REL_1_1GUAR_MINOR', 'REL_1_2GUAR', 'REL_1_3OWNER', 
          'REL_1_4BENEFI_OWNER', 'REL_1_5_SUPERVISOR', 'REL_2_ID', 'REL_2_TITLE', 
          'REL_2_1GUAR_MINOR', 'REL_2_2GUAR', 'REL_2_3OWNER', 'REL_2_4BENEFI_OWNER', 
          'REL_2_5_SUPERVISOR', 'REL_3_ID', 'REL_3_TITLE', 'REL_3_1GUAR_MINOR', 
          'REL_3_2GUAR', 'REL_3_3OWNER', 'REL_3_4BENEFI_OWNER', 'REL_3_5_SUPERVISOR', 
          'TEL1', 'TEL2'
      ],
      invalidate_hard_deletes=True,
      tags=["snapshot", "monthly_job"]
    )
}}

{% set target_date = var("target_date", "") %}


SELECT 
*
, CONVERT(
        VARCHAR(64), 
        HASHBYTES('SHA2_256', 
            CONCAT(ISNULL(ID, ''), '|', ISNULL(ID_DUP_NO, ''))
        ), 2) 
  AS CUST_PK
FROM {{ source('database', 'T_CUST') }}
WHERE 1=1
{% if target_date != "" %}
    AND TABLE_DATE = '{{ target_date }}'
{% else %}
    AND TABLE_DATE = (SELECT MAX(TABLE_DATE) FROM {{ source('database', 'T_CUST') }})
{% endif %}

{% endsnapshot %}