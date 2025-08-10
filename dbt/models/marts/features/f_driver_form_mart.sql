{{ config(schema='marts') }}
with res as (
  select driver_id, race_id, season, round, points,
         row_number() over (partition by driver_id order by season, round) as rn
  from raw.results r
  join raw.races ra using (race_id)
), roll as (
  select r1.driver_id, r1.race_id,
         avg(r2.points) as form_points_5
  from res r1
  join res r2 on r1.driver_id=r2.driver_id and r2.rn between r1.rn-5 and r1.rn-1
  group by 1,2
)
select * from roll