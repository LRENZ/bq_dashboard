-- 1. Daily weight coverage and normalization.
SELECT
  weight_date,
  COUNT(*) AS channels_with_weight,
  ROUND(SUM(weight), 8) AS weight_sum,
  MIN(window_start_date) AS window_start_date,
  MAX(window_end_date) AS window_end_date
FROM `bigquery-2024.attribution_v1.mta_markov_weights_daily`
WHERE weight_date BETWEEN DATE '2026-01-01' AND CURRENT_DATE("America/Los_Angeles")
GROUP BY weight_date
HAVING ABS(SUM(weight) - 1) > 0.0001 OR channels_with_weight = 0
ORDER BY weight_date;

-- 2. MTA totals must match last-click totals, allowing rounding noise.
SELECT
  date,
  SUM(last_click_conversions) AS lc_conversions,
  SUM(mta_conversions) AS mta_conversions,
  ROUND(SUM(mta_conversions) - SUM(last_click_conversions), 2) AS conversion_diff,
  SUM(last_click_revenue) AS lc_revenue,
  SUM(mta_revenue) AS mta_revenue,
  ROUND(SUM(mta_revenue) - SUM(last_click_revenue), 2) AS revenue_diff
FROM `bigquery-2024.attribution.composite_attribution_performance`
WHERE date BETWEEN DATE '2026-01-01' AND CURRENT_DATE("America/Los_Angeles")
GROUP BY date
HAVING ABS(conversion_diff) > 0.2 OR ABS(revenue_diff) > 1
ORDER BY date;

-- 3. Channels should no longer have one identical original Markov weight for every month.
SELECT
  channel,
  COUNT(DISTINCT FORMAT('%.8f', original_markov_weight)) AS distinct_original_weights,
  MIN(original_markov_weight) AS min_original_weight,
  MAX(original_markov_weight) AS max_original_weight
FROM `bigquery-2024.attribution.composite_attribution_performance`
WHERE date BETWEEN DATE '2026-01-01' AND DATE '2026-04-30'
  AND original_markov_weight > 0
GROUP BY channel
ORDER BY distinct_original_weights, channel;

