create schema if not exists raw;
create schema if not exists staging;
create schema if not exists marts;

-- Raw landing tables
create table if not exists raw.drivers (
  driver_id text primary key,
  code text, forename text, surname text, dob date, nationality text
);

create table if not exists raw.constructors (
  constructor_id text primary key,
  name text, nationality text
);

create table if not exists raw.races (
  race_id text primary key,
  season int, round int, circuit_id text, name text, date date, time text
);

create table if not exists raw.results (
  result_id text primary key,
  race_id text, driver_id text, constructor_id text,
  grid int, position int, points float, status text, fastest_lap int,
  time_ms bigint
);

create table if not exists raw.pitstops (
  id bigserial primary key,
  race_id text, driver_id text, stop int, lap int, duration_ms int
);

create table if not exists raw.qualifying (
  id bigserial primary key,
  race_id text, driver_id text, constructor_id text,
  q1_ms int, q2_ms int, q3_ms int, position int
);

create table if not exists raw.weather (
  race_id text primary key,
  summary text, is_wet boolean, temp_c numeric
);

-- Predictions
create table if not exists marts.f_predictions (
  season int, round int, race_id text, driver_id text,
  target text, prob_top10 numeric, pred_finish_pos numeric,
  model_version text, scored_at timestamptz default now()
);