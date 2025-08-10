import argparse
import pandas as pd
from xgboost import XGBClassifier, XGBRegressor
from utils.io import fetch_df, engine

FEATURE_VIEW = """
select ra.season, ra.round, r.race_id, r.driver_id,
       r.grid, f.form_points_5, p.team_quali_pos5, q.position as quali_pos,
       (r.grid - q.position) as grid_vs_quali, coalesce(w.is_wet, false) as is_wet
from raw.results r
join raw.races ra using (race_id)
left join staging.driver_form f using (race_id, driver_id)
left join staging.team_pace p using (race_id)
left join raw.qualifying q using (race_id, driver_id)
left join raw.weather w using (race_id)
where ra.season = %(season)s
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=["top10", "finish"], default="top10")
    parser.add_argument("--season", type=int, required=True)
    parser.add_argument("--round", type=str, default="all")
    args = parser.parse_args()

    df = fetch_df(FEATURE_VIEW % {"season": args.season})
    if args.round != "all":
        df = df[df["round"].astype(str) == str(args.round)]

    X = df[["grid","form_points_5","team_quali_pos5","quali_pos","grid_vs_quali","is_wet"]].fillna(0)

    if args.target == "top10":
        model = XGBClassifier(n_estimators=400, max_depth=5, learning_rate=0.05)
        model.fit(X, (df["quali_pos"]<=10).astype(int))  # cheap warm start; replace with saved model
        df["prob_top10"] = model.predict_proba(X)[:,1]
    else:
        model = XGBRegressor(n_estimators=600, max_depth=5, learning_rate=0.05)
        model.fit(X, df["quali_pos"])  # placeholder; replace with saved model
        df["pred_finish_pos"] = model.predict(X)

    # write predictions
    out = df[["season","round","race_id","driver_id"]].copy()
    out["target"] = args.target
    if args.target == "top10":
        out["prob_top10"] = df["prob_top10"]
        out["pred_finish_pos"] = None
    else:
        out["prob_top10"] = None
        out["pred_finish_pos"] = df["pred_finish_pos"]

    out["model_version"] = "v0.1-dev"

    with engine.begin() as conn:
        out.to_sql("f_predictions", conn, schema="marts", if_exists="append", index=False)
    print("Predictions written to marts.f_predictions")