{{ config(materialized='view', schema='marts') }}
select ra.season, ra.round, r.race_id, r.driver_id,
       gc.grid, df.form_points_5, tp.team_quali_pos5, gc.quali_pos,
       gc.grid_vs_quali, coalesce(w.is_wet,false) as is_wet
from raw.races ra
join raw.results r using (race_id)
left join {{ ref('f_driver_form_mart') }} df using (race_id, driver_id)
left join {{ ref('f_team_pace_mart') }} tp using (race_id)
left join {{ ref('f_grid_context_mart') }} gc using (race_id, driver_id)
left join {{ ref('f_weather_mart') }} w using (race_id)