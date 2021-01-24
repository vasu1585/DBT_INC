{{
   config(
       materialized='table',
   )
}}
SELECT
    MD.SENSOR_IDX as SNSR_IDX,
    MAX(METER.EVENT_TS)
    END  AS MAX_TS
    FROM   {{ source('dbt_EVENTS_src', 'EVENTS') }} METER LEFT JOIN {{ source('dbt_watermark', 'MASTERDATA') }} MD

    on MD.SENSOR_IDX = METER.SENSOR_IDX
    GROUP BY METER.METER_NUMBER_INDEX