
# f1-predictor-bi
End to end F1 race outcome prediction and BI platform with Ergast ingestion, dbt feature marts, XGBoost models, and Power BI/Tableau dashboards.

# F1 Race Outcome Prediction + BI Platform
End‑to‑end project: Ergast ingestion → Postgres → dbt feature marts → XGBoost models → Power BI/Tableau dashboards. Built for a Business Intelligence Engineer portfolio.

## Architecture
- Ingest (Python) → Warehouse (Postgres) → Transform (dbt) → Train/Score (XGBoost) → BI (Power BI)
- Orchestrated by Airflow (optional).

## How to Run
1. Configure `.env` and run `docker compose up -d` (or use your own Postgres)
2. `psql -f schema.sql`
3. `pip install -r requirements.txt`
4. `python src/ingest/ergast_pull.py --from-season 2014 --to-season 2024`
5. `cd dbt && dbt build`
6. `python src/models/train.py --target top10`
7. `python src/models/predict.py --target top10 --season 2024 --round all`
8. Connect BI to Postgres and explore `marts.f_predictions`.

## Highlights
- Dimensional marts, realistic KPIs, calibration metrics, ops page.
- Clean repo layout, easy to extend (safety car model, pit strategy simulator).


