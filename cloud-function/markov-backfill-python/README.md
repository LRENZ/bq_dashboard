# Markov Backfill Cloud Function

This function backfills daily Markov weights into:

`bigquery-2024.attribution_v1.mta_markov_weights_daily`

It reuses the same GA4 event query and Markov algorithm as the daily function, but it does not trigger Dataform after each date.

## Recommended Entry Point

`backfill_monthly_markov`

This entry point calculates one Markov weight set per month, then writes the same weights to every reporting date in that month.

The older daily entry point `backfill_markov` is still available, but it is more expensive.

## Recommended Environment Variables

```text
PROJECT_ID=bigquery-2024
TRIGGER_DATAFORM_AFTER=false
BACKFILL_MAX_DAYS=7
BACKFILL_MAX_MONTHS=3
DATAFORM_REGION=us-central1
DATAFORM_REPOSITORY_ID=bq
DATAFORM_WORKFLOW_CONFIG_ID=ga4_attribution
```

## Request Parameters

- `monthlyStart`: optional, defaults to `2025-09-01`
- `monthlyEnd`: optional, defaults to `2026-05-31`
- `fallbackStart`: optional. Defaults to `channel_performance` min date. Dates before `monthlyStart` use the `monthlyStart` month weights.
- `maxMonths`: optional. Default comes from `BACKFILL_MAX_MONTHS`. Use `0` for no application-level limit, but this can hit the Cloud Function timeout.
- `triggerDataformAfter`: optional. Use `true` only on the final batch if you want this function to trigger Dataform once.

## Example Calls

First batch:

```text
https://REGION-PROJECT.cloudfunctions.net/FUNCTION_NAME?monthlyStart=2025-09-01&monthlyEnd=2026-05-31&maxMonths=3
```

The response contains `nextMonthlyStart` when more months remain. Call again with:

```text
https://REGION-PROJECT.cloudfunctions.net/FUNCTION_NAME?monthlyStart=<nextMonthlyStart>&monthlyEnd=2026-05-31&maxMonths=3
```

Final batch, optionally triggering Dataform once:

```text
https://REGION-PROJECT.cloudfunctions.net/FUNCTION_NAME?monthlyStart=<lastNextMonthlyStart>&monthlyEnd=2026-05-31&maxMonths=3&triggerDataformAfter=true
```

## Recommendation

For a long historical backfill, running `scripts/markov_backfill_python.py` locally is safer because it avoids Cloud Function request timeout limits. Use this Cloud Function for small batches or when you need a fully cloud-side workflow.
