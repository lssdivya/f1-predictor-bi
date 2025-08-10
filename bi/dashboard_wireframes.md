Pages:
1) Pre‑Race Predictions
   - Slicers: Season, Race, Team, Driver, Weather
   - Cards: Model Version, Data Freshness, Calibration Brier, AUC
   - Table: Driver | Grid | Top‑10 Prob | Expected Finish | Confidence
   - What‑if: Grid Adjustment (‑2..+2), Wet Toggle

2) Strategy & Pit Stop Impact (stretch)
   - Scatter: Team pit‑stop avg vs Top‑10 probability delta
   - Slider: Pit stop improvement (‑0.4s) → recompute expected finish (calc table)

3) Model Quality
   - Reliability chart (bins of predicted prob vs actual rate)
   - MAE by season & by circuit
   - Feature importance (image from SHAP)

4) Ops
   - Pipeline status, last successful run, rows by table, DB size