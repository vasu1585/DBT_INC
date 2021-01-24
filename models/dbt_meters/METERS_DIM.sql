{{
    config(
        materialized='table',
        database="DEV_EDW"
    )
}}



SELECT
*
FROM {{ref ('METERS_SOURCE')}} SRC 

