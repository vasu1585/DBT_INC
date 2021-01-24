{{
   config(
      -- *optional* To delete the data from the target table which are older than last 365 days
       pre_hook=before_begin("DELETE FROM {{ source('Target', 'EVENTS') }} where DATE  < current_date - 365"),
 	     materialized='incremental',
       unique_key="id",
         )
}}
 
SELECT
SNSR_IDX as id, *
FROM   {{ref ('events_source')}} SRC JOIN {{ref ('WATERMARK')}} WK 
ON SRC.SENSOR_IDX = WK.SENSOR_IDX
 
{% if is_incremental() %}
 
 -- this filter will only be applied on an incremental run
 where SRC.EVENT_TS >= WK.MAX_TS 
 
{% endif %}
