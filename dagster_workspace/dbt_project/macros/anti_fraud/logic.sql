/*
Created: 2026-05-22
Description: 
Change Log:
- 2026-05-22 [REFACT] 定義出入帳通用篩選條件
*/


{# 【入帳/不排小額】純看代碼 #}
{% macro is_inward_all(model_name) %}
    {% set cfg = get_config()[model_name] %}
    
    (
        DR_FLG = '0' 
        AND FUNC_CODE IN {{ cfg['inward_codes'] }}
    )
{% endmacro %}


{# 【入帳/排除小額】看代碼 + 吃 Config 裡的金額門檻 #}
{% macro is_inward_large(model_name) %}
    {% set cfg = get_config()[model_name] %}
    
    (
        DR_FLG = '0' 
        AND TXN_AMT > {{ cfg['inward_threshold'] }} 
        AND FUNC_CODE IN {{ cfg['inward_codes'] }}
    )
{% endmacro %}

{# 【出帳/不排小額】純看代碼 #}
{% macro is_outward_all(model_name) %}
    {% set cfg = get_config()[model_name] %}
    
    (
        DR_FLG = '1' 
        AND FUNC_CODE IN {{ cfg['outward_codes'] }}
    )
{% endmacro %}


{# 【出帳/排除小額】看代碼 + 吃 Config 裡的金額門檻 #}
{% macro is_outward_large(model_name) %}
    {% set cfg = get_config()[model_name] %}
    
    (
        DR_FLG = '1' 
        AND TXN_AMT > {{ cfg['outwardthreshold'] }} 
        AND FUNC_CODE IN {{ cfg['outward_codes'] }}
    )
{% endmacro %}

{# 【ATM出帳/排除小額】看代碼 + 吃 Config 裡的金額門檻 #}
{% macro is_atm_outward_all(model_name) %}
    {% set cfg = get_config()[model_name] %}
    
    (
        DR_FLG = '1' 
        AND FUNC_CODE IN {{ cfg['atm_outward_codes'] }}
    )
{% endmacro %}