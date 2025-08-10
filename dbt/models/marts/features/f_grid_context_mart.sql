{{ config(schema='marts') }}
select r.race_id, r.driver_id, r.grid, q.position as quali_pos,
       (r.grid - q.position) as grid_vs_quali
from raw.results r
left join raw.qualifying q using (race_id, driver_id)