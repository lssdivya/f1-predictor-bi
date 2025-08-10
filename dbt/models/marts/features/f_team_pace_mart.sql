{{ config(schema='marts') }}
with q as (
  select constructor_id, race_id, position,
         row_number() over (partition by constructor_id order by race_id) as rn
  from raw.qualifying
), roll as (
  select q1.constructor_id, q1.race_id, avg(q2.position)::float as team_quali_pos5
  from q q1 join q q2 on q1.constructor_id=q2.constructor_id and q2.rn between q1.rn-5 and q1.rn-1
  group by 1,2
)
select * from roll