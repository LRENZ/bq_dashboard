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
pip install -r cloud-function/markov-python/requirements.txt
python scripts/markov_monthly_backfill.py --monthly-start=2025-09-01 --monthly-end=2026-05-31 --keyfile="C:/Users/HP/Downloads/bigquery-2024-d889a9800188.json"
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

## Cloud Function Backfill Option

If you do not want to run the local script, deploy `cloud-function/markov-backfill-python` with entry point `backfill_monthly_markov`.

Run it in batches:

```text
?monthlyStart=2025-09-01&monthlyEnd=2026-05-31&maxMonths=3
```

Use the returned `nextMonthlyStart` for the next request. On the final request, you can add:

```text
&triggerDataformAfter=true
```

For long ranges, local execution is safer than one Cloud Function request because Cloud Functions have request timeout limits.

## Daily Forward Flow

1. Dataform refreshes `session_source_medium` for recent purchase dates.
2. Cloud Scheduler calls the replacement Cloud Function in `cloud-function/markov-daily`.
3. The function MERGEs yesterday's weights into `mta_markov_weights_daily`.
4. Dataform runs the downstream attribution tables incrementally.

## Important Guardrail

Do not use `attribution_v1.mta_attribution_v1_markov_details` as the reporting weight source. That table is loaded with `WRITE_TRUNCATE` and only represents the latest run.
