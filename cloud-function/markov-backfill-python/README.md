# Markov Backfill Cloud Function

This function backfills daily Markov weights into:

`bigquery-2024.attribution_v1.mta_markov_weights_daily`

It reuses the same GA4 event query and Markov algorithm as the daily function, but it does not trigger Dataform after each date.

## Entry Point

`backfill_markov`

## Recommended Environment Variables

```text
PROJECT_ID=bigquery-2024
TRIGGER_DATAFORM_AFTER=false
BACKFILL_MAX_DAYS=7
DATAFORM_REGION=us-central1
DATAFORM_REPOSITORY_ID=bq
DATAFORM_WORKFLOW_CONFIG_ID=ga4_attribution
```

## Request Parameters

- `startDate`: required, `YYYY-MM-DD`
- `endDate`: required, `YYYY-MM-DD`
- `maxDays`: optional. Default comes from `BACKFILL_MAX_DAYS`. Use `0` for no application-level limit, but this can hit the Cloud Function timeout.
- `triggerDataformAfter`: optional. Use `true` only on the final batch if you want this function to trigger Dataform once.

## Example Calls

First batch:

```text
https://REGION-PROJECT.cloudfunctions.net/FUNCTION_NAME?startDate=2026-01-01&endDate=2026-06-01&maxDays=7
```

The response contains `nextStartDate` when more dates remain. Call again with:

```text
https://REGION-PROJECT.cloudfunctions.net/FUNCTION_NAME?startDate=<nextStartDate>&endDate=2026-06-01&maxDays=7
```

Final batch, optionally triggering Dataform once:

```text
https://REGION-PROJECT.cloudfunctions.net/FUNCTION_NAME?startDate=<lastNextStartDate>&endDate=2026-06-01&maxDays=7&triggerDataformAfter=true
```

## Recommendation

For a long historical backfill, running `scripts/markov_backfill_python.py` locally is safer because it avoids Cloud Function request timeout limits. Use this Cloud Function for small batches or when you need a fully cloud-side workflow.

