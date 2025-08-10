{{ config(materialized='table', schema='marts') }}
select * from marts.f_predictions