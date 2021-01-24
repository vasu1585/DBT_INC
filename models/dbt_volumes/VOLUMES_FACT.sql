{{
    config(
       pre_hook=before_begin("DELETE FROM {{ source('Target', 'VOLUMES_FACT') }} where MEASUREMENT_DATE  < current_date - 365"),
        materialized='incremental',
        unique_key="id",
        database="DEV_EDW",
        schema="DBT_SRINI"
    )
}}

SELECT
{{ dbt_utils.surrogate_key('SRC.METER_NUMBER_INDEX', 'SRC.MEASUREMENT_DAY', 'SRC.PRODUCT_INDEX', 'SRC.CLASS_RELATIONSHIP_IDX') }} as id, *
FROM {{ref ('VOLUMES_SOURCE')}} SRC JOIN {{ref ('WATERMARK')}} WK 
ON SRC.METER_NUMBER_INDEX = WK.MTR_NBR_INDX

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  where SRC.GMS_DATE >= WK.GMSDATE  

{% endif %}

