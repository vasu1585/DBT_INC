{{
    config(
        materialized='table', 
        database="DEV_EDW",
		transient=false,
        schema="DBT_SRINI"
    )
}}

SELECT
	MTIMESERIES.SENSOR_IDX,
	MTIMESERIES.MEASUREMENT_DAY,
	MTIMESERIES.PRODUCT_INDEX,
	CLASS_RELATIONSHIP_IDX,
	MTIMESERIES.EVENT_TS,
	MTIMESERIES.PRESSURE,
	MTIMESERIES.C6,
	MTIMESERIES.C7,
	MTIMESERIES.C8,
	MTIMESERIES.C9,
	MTIMESERIES.C10,
	MTIMESERIES.CO,
	MTIMESERIES.CO2,
	MTIMESERIES.H2,
	MTIMESERIES.H2O,
	MTIMESERIES.H2S,
	MTIMESERIES.HE,
	MTIMESERIES.N2,
	MTIMESERIES.O2, 
	MTIMESERIES.TIME_STAMP
	      FROM 
    {{ source('dbt_volumes_source', 'MASTERDATA') }} METER
      LEFT OUTER  JOIN {{ source('dbt_volumes_source', 'FC_EVENT_DLY') }} MTIMESERIES ON METER.SENSOR_IDX = MTIMESERIES.SENSOR_IDX
	  -- Anytime this will load only last 365 days data
	  and MTIMESERIES.DATE between DATEADD(Day ,-365, current_date) and current_date
      