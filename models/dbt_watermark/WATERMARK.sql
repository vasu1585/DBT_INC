{{
    config(
        materialized='table', 
        database="DEV_EDW",
        schema="DBT_SRINI"
    )
}}

SELECT 
     METER.METER_NUMBER_INDEX as MTR_NBR_INDX,
      CASE 
        WHEN MAX(VOL.GMS_DATE) IS NULL THEN  current_date - 730
        ELSE MAX(VOL.GMS_DATE)
     END  AS GMSDATE
     FROM   {{ source('dbt_volumes_src', 'FC_METER') }} METER LEFT JOIN {{ source('dbt_enable_watermark', 'VOLUMES_FACT') }} VOL
     on METER.METER_NUMBER_INDEX = VOL.METER_NUMBER_INDEX
     GROUP BY METER.METER_NUMBER_INDEX



     {{
   config(
       materialized='incremental'
   )
}}
 
select
   *
from raw_app_data.events
 
{% if is_incremental() %}
 -- this filter will only be applied on an incremental run
 where event_time > (select max(event_time) from {{ this }})
{% endif %}