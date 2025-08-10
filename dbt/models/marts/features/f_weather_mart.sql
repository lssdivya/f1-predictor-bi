{{ config(schema='marts') }}
select race_id, coalesce(is_wet,false) as is_wet, temp_c
from raw.weather