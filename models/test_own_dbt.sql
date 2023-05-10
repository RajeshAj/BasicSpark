{{ config(materialized="table") }} select * from ANALYTICS_LOAD.DEAL_DATA.UAT_DEAL2_V1 where datetime >= current_date() 
