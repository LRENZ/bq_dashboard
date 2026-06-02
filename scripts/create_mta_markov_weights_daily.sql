CREATE TABLE IF NOT EXISTS `bigquery-2024.attribution_v1.mta_markov_weights_daily` (
  weight_date DATE NOT NULL,
  window_start_date DATE NOT NULL,
  window_end_date DATE NOT NULL,
  channel STRING NOT NULL,
  removal_effect FLOAT64,
  weight FLOAT64,
  run_timestamp TIMESTAMP NOT NULL
)
PARTITION BY weight_date
CLUSTER BY channel;

