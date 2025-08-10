import argparse
import pandas as pd
from sklearn.model_selection import GroupKFold
from sklearn.metrics import roc_auc_score, brier_score_loss, mean_absolute_error
from sklearn.calibration import CalibratedClassifierCV
from xgboost import XGBClassifier, XGBRegressor
from utils.io import fetch_df

FEATURE_VIEW = """
select ra.season, ra.round, r.race_id, r.driver_id,
       r.grid,
       f.form_points_5,
       p.team_quali_pos5,
       q.position as quali_pos,
       (r.grid - q.position) as grid_vs_quali,
       coalesce(w.is_wet, false) as is_wet,
       (r.position between 1 and 10) as is_top10,
       r.position as finish_pos
from raw.results r
join raw.races ra using (race_id)
left join staging.driver_form f using (race_id, driver_id)
left join staging.team_pace p using (race_id)
left join raw.qualifying q using (race_id, driver_id)
left join raw.weather w using (race_id)
where r.position is not null
"""

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", choices=["top10", "finish"], default="top10")
    args = parser.parse_args()

    df = fetch_df(FEATURE_VIEW)

    features = ["grid", "form_points_5", "team_quali_pos5", "quali_pos", "grid_vs_quali", "is_wet"]

    if args.target == "top10":
        X = df[features].fillna(0)
        y = df["is_top10"].astype(int)
        groups = df["season"]
        gkf = GroupKFold(n_splits=5)
        preds, trues = [], []
        for tr, va in gkf.split(X, y, groups):
            model = XGBClassifier(n_estimators=400, max_depth=5, learning_rate=0.05, subsample=0.8, colsample_bytree=0.9, reg_lambda=2.0)
            clf = CalibratedClassifierCV(model, method="isotonic", cv=3)
            clf.fit(X.iloc[tr], y.iloc[tr])
            p = clf.predict_proba(X.iloc[va])[:,1]
            preds.extend(p)
            trues.extend(y.iloc[va].tolist())
        auc = roc_auc_score(trues, preds)
        brier = brier_score_loss(trues, preds)
        print({"cv_auc": auc, "cv_brier": brier})
    else:
        X = df[features].fillna(0)
        y = df["finish_pos"].astype(int)
        groups = df["season"]
        gkf = GroupKFold(n_splits=5)
        preds, trues = [], []
        for tr, va in gkf.split(X, y, groups):
            model = XGBRegressor(n_estimators=600, max_depth=5, learning_rate=0.05, subsample=0.8, colsample_bytree=0.9, reg_lambda=2.0)
            model.fit(X.iloc[tr], y.iloc[tr])
            p = model.predict(X.iloc[va])
            preds.extend(p)
            trues.extend(y.iloc[va].tolist())
        mae = float(mean_absolute_error(trues, preds))
        print({"cv_mae": mae})