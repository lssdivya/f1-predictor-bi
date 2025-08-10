**Top10 Probability (card)** = AVERAGE(marts_f_predictions[prob_top10])

**Calibration – Reliability Plot**
Bucket = FLOOR(marts_f_predictions[prob_top10]*10)/10
ActualRate = DIVIDE(CALCULATE(COUNTROWS(FILTER(marts_f_predictions, [is_top10]=TRUE())), ALL(marts_f_predictions)), COUNTROWS(ALL(marts_f_predictions)))

**Backtest MAE by Season** = AVERAGEX(VALUES(marts_f_predictions[season]), [Season_MAE])