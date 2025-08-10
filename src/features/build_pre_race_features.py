import pandas as pd
from utils.io import fetch_df, upsert_df

# Build rolling form, team pace, grid context, and weather joins into staging tables

# Driver form: rolling points last 5 races before current race
form_sql = """
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
"""

team_pace_sql = """
-- proxy: average qualifying position by constructor over last 5 events
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
"""

if __name__ == "__main__":
    form = fetch_df(form_sql)
    upsert_df(form, "driver_form", schema="staging")

    pace = fetch_df(team_pace_sql)
    upsert_df(pace, "team_pace", schema="staging")

    print("Feature staging built.")