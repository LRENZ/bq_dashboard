# MTA Daily Weight Backfill Runbook

This migration fixes historical MTA attribution by storing one Markov weight set per reporting date.

## What Changed

- `attribution_v1.mta_markov_weights_daily` stores daily Markov weights.
- `attribution_v1.mta_weights_complete` now includes `Weight_Date`.
- `attribution.composite_attribution_performance` joins weights by `date + channel`.
- `attribution.session_source_medium` now keeps `purchase_date` and `transaction_id`, so backfills can train on each target date's trailing 30-day journey window.

## Safe Execution Order

1. Create the daily weight history table:

```sql
-- Run scripts/create_mta_markov_weights_daily.sql in BigQuery.
```

2. Rebuild `attribution.session_source_medium` once.

This table changed schema and partitioning. Run only this Dataform action first. A controlled full rebuild is expected for this table during the migration.

3. Backfill daily Markov weights.

From a machine with Node.js and BigQuery credentials:

```bash
npm install @google-cloud/bigquery
node scripts/markov_backfill.js --start=2026-01-01 --end=2026-06-01 --keyfile="C:/Users/HP/Downloads/bigquery-2024-d889a9800188.json"
```

Adjust `--end` to the last date you want to backfill.

4. Run these Dataform actions in order:

- `bigquery-2024.attribution_v1.mta_weights_complete`
- `bigquery-2024.attribution.composite_attribution_performance`
- `bigquery-2024.attribution.composite_attribution_reporting`
- `bigquery-2024.attribution.composite_campaign_attribution_performance`

5. Run validation:

```sql
-- Run scripts/validate_mta_fix.sql in BigQuery.
```

The first two validation queries should return zero rows. The third query is diagnostic: high `distinct_original_weights` means historical months are no longer using one frozen global weight.

## Daily Forward Flow

1. Dataform refreshes `session_source_medium` for recent purchase dates.
2. Cloud Scheduler calls the replacement Cloud Function in `cloud-function/markov-daily`.
3. The function MERGEs yesterday's weights into `mta_markov_weights_daily`.
4. Dataform runs the downstream attribution tables incrementally.

## Important Guardrail

Do not use `attribution_v1.mta_attribution_v1_markov_details` as the reporting weight source. That table is loaded with `WRITE_TRUNCATE` and only represents the latest run.

