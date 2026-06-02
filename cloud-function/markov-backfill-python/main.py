import json
import logging
import os
import traceback
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd
from google.cloud import bigquery
from google.cloud import dataform_v1beta1
from google.api_core import exceptions as google_exceptions


class Config:
    PROJECT_ID = os.environ.get("PROJECT_ID", "bigquery-2024")
    DEST_DATASET_ID = os.environ.get("DEST_DATASET_ID", "attribution_v1")
    SOURCE_EVENTS_TABLE = os.environ.get(
        "SOURCE_EVENTS_TABLE",
        "bigquery-2024.analytics_489554557.events_*",
    )
    DAILY_WEIGHT_TABLE = os.environ.get(
        "DAILY_WEIGHT_TABLE",
        "bigquery-2024.attribution_v1.mta_markov_weights_daily",
    )
    LATEST_DETAILS_TABLE = os.environ.get(
        "LATEST_DETAILS_TABLE",
        "bigquery-2024.attribution_v1.mta_attribution_v1_markov_details",
    )
    BQ_LOCATION = os.environ.get("BQ_LOCATION", "US")

    # Backfills should not trigger Dataform after every single date.
    # Pass triggerDataformAfter=true in the final request to trigger once.
    TRIGGER_DATAFORM_AFTER = os.environ.get("TRIGGER_DATAFORM_AFTER", "false").lower() == "true"
    BACKFILL_MAX_DAYS = int(os.environ.get("BACKFILL_MAX_DAYS", "7"))
    BACKFILL_MAX_MONTHS = int(os.environ.get("BACKFILL_MAX_MONTHS", "3"))
    DATAFORM_REGION = os.environ.get("DATAFORM_REGION", "us-central1")
    DATAFORM_REPOSITORY_ID = os.environ.get("DATAFORM_REPOSITORY_ID", "bq")
    DATAFORM_WORKFLOW_CONFIG_ID = os.environ.get("DATAFORM_WORKFLOW_CONFIG_ID", "ga4_attribution")

    MARKOV_EPSILON = 1e-10
    MARKOV_CONDITION_THRESHOLD = 1e10


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


ATTRIBUTION_QUERY = """
WITH RawEvents AS (
  SELECT
    user_pseudo_id,
    user_id,
    event_timestamp,
    event_name,
    event_params,
    session_traffic_source_last_click,
    ecommerce,
    (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS ga_session_id
  FROM `{source_events_table}`
  WHERE event_name IN ('purchase', 'session_start', 'first_visit', 'page_view')
    AND _TABLE_SUFFIX BETWEEN FORMAT_DATE('%Y%m%d', @window_start_date)
                          AND FORMAT_DATE('%Y%m%d', @target_date)
),

Session_Windowed AS (
  SELECT
    user_pseudo_id,
    user_id,
    event_timestamp,
    event_name,
    ga_session_id,
    ecommerce.transaction_id,
    COALESCE(ecommerce.purchase_revenue_in_usd, 0) AS transaction_value,
    FIRST_VALUE(
      LOWER(COALESCE(
        session_traffic_source_last_click.cross_channel_campaign.source,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'source')
      )) IGNORE NULLS
    ) OVER (
      PARTITION BY user_pseudo_id, ga_session_id
      ORDER BY event_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS session_source,
    FIRST_VALUE(
      LOWER(COALESCE(
        session_traffic_source_last_click.cross_channel_campaign.medium,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'medium')
      )) IGNORE NULLS
    ) OVER (
      PARTITION BY user_pseudo_id, ga_session_id
      ORDER BY event_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS session_medium,
    FIRST_VALUE(
      LOWER(COALESCE(
        session_traffic_source_last_click.cross_channel_campaign.campaign_name,
        (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'campaign')
      )) IGNORE NULLS
    ) OVER (
      PARTITION BY user_pseudo_id, ga_session_id
      ORDER BY event_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS session_campaign,
    FIRST_VALUE(
      session_traffic_source_last_click.cross_channel_campaign.default_channel_group IGNORE NULLS
    ) OVER (
      PARTITION BY user_pseudo_id, ga_session_id
      ORDER BY event_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS raw_channel_group,
    FIRST_VALUE(
      CASE
        WHEN event_name IN ('page_view', 'session_start')
        THEN LOWER((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'))
      END IGNORE NULLS
    ) OVER (
      PARTITION BY user_pseudo_id, ga_session_id
      ORDER BY event_timestamp
      ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS landing_page
  FROM RawEvents
),

SessionEvents AS (
  SELECT
    COALESCE(
      LAST_VALUE(user_id IGNORE NULLS) OVER (
        PARTITION BY user_pseudo_id
        ORDER BY event_timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ),
      user_pseudo_id
    ) AS unified_user_id,
    event_timestamp,
    event_name,
    COALESCE(raw_channel_group, 'Unassigned') AS default_channel_group,
    COALESCE(session_source, '(direct)') AS source,
    COALESCE(session_medium, '(none)') AS medium,
    COALESCE(session_campaign, '(not set)') AS campaign,
    landing_page,
    transaction_id,
    transaction_value
  FROM Session_Windowed
),

EventsWithFlags AS (
  SELECT
    *,
    CASE
      WHEN REGEXP_CONTAINS(default_channel_group, r'Paid') THEN TRUE
      WHEN default_channel_group = 'Display' THEN TRUE
      WHEN REGEXP_CONTAINS(medium, r'^(cpc|ppc|display|cpm|banner)$') THEN TRUE
      WHEN medium LIKE '%paid%' THEN TRUE
      ELSE FALSE
    END AS is_initial_paid_flag
  FROM SessionEvents
),

BaseTouchpoints AS (
  SELECT
    unified_user_id,
    event_timestamp,
    event_name,
    source,
    medium,
    campaign,
    landing_page,
    transaction_id,
    transaction_value,
    is_initial_paid_flag,
    CASE
      WHEN is_initial_paid_flag
           AND REGEXP_CONTAINS(landing_page, r'/pages/(what-is-an-ebike|cruiser-ebike|folding-ebike|city-ebike|fat-tire-ebike)')
        THEN 'pillar page'
      WHEN default_channel_group = 'Affiliates'
        OR REGEXP_CONTAINS(medium, r'affiliate|avantlink|uppromote|goaffpro|impact')
        OR REGEXP_CONTAINS(source, r'affiliate|avantlink|uppromote|goaffpro|impact')
        THEN 'affiliates'
      WHEN default_channel_group = 'SMS' OR REGEXP_CONTAINS(medium, r'sms') THEN 'sms'
      WHEN default_channel_group = 'Email'
        OR REGEXP_CONTAINS(source, r'klaviyo|email|substack|wunderkind')
        OR medium = 'edm'
        THEN 'email'
      WHEN REGEXP_CONTAINS(medium, r'kol') OR source = 'kol' THEN 'kol'
      WHEN medium = 'programmatic'
        OR REGEXP_CONTAINS(source, r'ttd|outbrain|loopme|criteo|tradedesk')
        THEN 'programmatic ads'
      WHEN (default_channel_group = 'Paid Search' OR medium IN ('cpc', 'ppc', 'paidsearch'))
        AND REGEXP_CONTAINS(campaign, r'brand')
        THEN 'paid brand search'
      WHEN default_channel_group = 'Paid Search'
        AND NOT REGEXP_CONTAINS(campaign, r'brand')
        THEN 'paid non-brand search'
      WHEN default_channel_group = 'Organic Search' THEN 'organic search'
      WHEN (
          default_channel_group = 'Paid Social'
          OR REGEXP_CONTAINS(source, r'facebook|twitter|youtube|instagram|reddit|fb|quora')
          OR medium = 'paidsocial'
        )
        AND medium != 'referral'
        THEN 'paid social'
      WHEN default_channel_group = 'Organic Social' THEN 'organic social'
      WHEN default_channel_group = 'Paid Video' OR medium = 'paidvideo' THEN 'paid video'
      WHEN default_channel_group = 'Organic Video' THEN 'organic video'
      WHEN default_channel_group = 'Paid Shopping' THEN 'paid shopping'
      WHEN default_channel_group = 'Organic Shopping' THEN 'organic shopping'
      WHEN default_channel_group = 'Display' OR medium = 'paiddisplay' THEN 'paid display'
      WHEN default_channel_group = 'Cross-network' THEN 'cross-network'
      WHEN default_channel_group = 'Audio' THEN 'audio'
      WHEN default_channel_group = 'Mobile Push Notifications' OR REGEXP_CONTAINS(medium, r'push') THEN 'mobile push notifications'
      WHEN default_channel_group = 'Paid Other' THEN 'paid other'
      WHEN REGEXP_CONTAINS(medium, r'ads|pr|paidreferral') THEN 'paid referral'
      WHEN default_channel_group = 'Referral'
        OR REGEXP_CONTAINS(source, r'chatgpt')
        OR REGEXP_CONTAINS(medium, r'earnedreferral')
        THEN 'organic referral'
      WHEN default_channel_group = 'Direct'
        OR source = '(direct)'
        OR medium IN ('(none)', 'none')
        THEN 'direct'
      WHEN REGEXP_CONTAINS(medium, r'tv|television|connectedtv')
        OR REGEXP_CONTAINS(source, r'mntn')
        THEN 'connected tv'
      ELSE 'unassigned'
    END AS base_channel
  FROM EventsWithFlags
),

AllTouchpoints AS (
  SELECT
    unified_user_id,
    event_timestamp,
    event_name,
    source,
    medium,
    campaign,
    transaction_id,
    transaction_value,
    CASE
      WHEN base_channel = 'affiliates' THEN FALSE
      WHEN REGEXP_CONTAINS(base_channel, r'^paid') THEN TRUE
      WHEN base_channel IN ('programmatic ads', 'connected tv', 'display', 'cross-network') THEN TRUE
      WHEN is_initial_paid_flag THEN TRUE
      ELSE FALSE
    END AS is_paid_traffic,
    CASE
      WHEN base_channel IN ('pillar page', 'email', 'sms', 'affiliates', 'unassigned', 'kol') THEN base_channel
      WHEN is_initial_paid_flag
           OR REGEXP_CONTAINS(base_channel, r'^paid')
           OR base_channel IN ('programmatic ads', 'connected tv', 'display', 'cross-network')
      THEN
        CASE
          WHEN CONTAINS_SUBSTR(campaign, '-aw') THEN base_channel || ':branding'
          WHEN CONTAINS_SUBSTR(campaign, '-cs') THEN base_channel || ':traffic'
          WHEN CONTAINS_SUBSTR(campaign, '-cv') THEN base_channel || ':conversion'
          WHEN CONTAINS_SUBSTR(campaign, '-rt') THEN base_channel || ':retargeting'
          ELSE base_channel || ':conversion'
        END
      ELSE base_channel
    END AS channel
  FROM BaseTouchpoints
),

PurchaseEvents AS (
  SELECT
    unified_user_id,
    transaction_id,
    event_timestamp AS purchase_timestamp,
    transaction_value,
    DATE(TIMESTAMP_MICROS(event_timestamp), "America/Los_Angeles") AS purchase_date,
    LAG(event_timestamp, 1, 0) OVER (
      PARTITION BY unified_user_id
      ORDER BY event_timestamp
    ) AS previous_purchase_timestamp
  FROM AllTouchpoints
  WHERE transaction_id IS NOT NULL
    AND DATE(TIMESTAMP_MICROS(event_timestamp), "America/Los_Angeles")
      BETWEEN @window_start_date AND @target_date
),

TransactionJourneys AS (
  SELECT
    p.unified_user_id,
    p.transaction_id,
    p.purchase_date,
    p.transaction_value,
    ARRAY_AGG(
      STRUCT(
        t.channel,
        t.event_name,
        t.event_timestamp,
        t.is_paid_traffic,
        t.source,
        t.medium,
        t.campaign
      )
      ORDER BY t.event_timestamp
    ) AS touchpoint_path
  FROM PurchaseEvents p
  JOIN AllTouchpoints t
    ON p.unified_user_id = t.unified_user_id
  WHERE t.event_timestamp > p.previous_purchase_timestamp
    AND t.event_timestamp <= p.purchase_timestamp
  GROUP BY 1, 2, 3, 4
),

ExpandedPath AS (
  SELECT
    t.unified_user_id,
    t.purchase_date,
    t.transaction_id,
    TIMESTAMP_MICROS(touchpoint.event_timestamp) AS event_date,
    touchpoint.event_name,
    touchpoint.channel,
    touchpoint.is_paid_traffic,
    touchpoint.source,
    touchpoint.medium,
    touchpoint.campaign,
    t.transaction_value
  FROM TransactionJourneys t
  CROSS JOIN UNNEST(t.touchpoint_path) AS touchpoint
),

PathWithTransactions AS (
  SELECT
    unified_user_id,
    purchase_date,
    transaction_id,
    event_date,
    channel,
    is_paid_traffic,
    source,
    medium,
    campaign,
    CASE WHEN event_name = 'purchase' THEN 1 ELSE 0 END AS total_transactions,
    CASE WHEN event_name = 'purchase' THEN transaction_value ELSE 0 END AS total_transaction_value,
    CASE WHEN event_name = 'purchase' THEN TRUE ELSE FALSE END AS has_transaction,
    LAG(channel) OVER (
      PARTITION BY unified_user_id, transaction_id
      ORDER BY event_date
    ) AS prev_channel,
    event_name
  FROM ExpandedPath
),

FinalOutput AS (
  SELECT
    unified_user_id,
    transaction_id,
    event_date,
    channel,
    total_transactions,
    total_transaction_value,
    has_transaction
  FROM PathWithTransactions
  WHERE prev_channel IS NULL
    OR channel != prev_channel
    OR event_name = 'purchase'
)

SELECT
  unified_user_id,
  transaction_id,
  event_date,
  channel,
  SUM(total_transactions) AS total_transactions,
  SUM(total_transaction_value) AS total_transaction_value,
  MAX(has_transaction) AS has_transaction
FROM FinalOutput
GROUP BY
  unified_user_id,
  transaction_id,
  event_date,
  channel
ORDER BY unified_user_id, transaction_id, event_date
"""


def parse_target_date(request):
    if hasattr(request, "get_json"):
        payload = request.get_json(silent=True) or {}
        query_target = request.args.get("targetDate") if request.args else None
        target = query_target or payload.get("targetDate")
    elif isinstance(request, dict):
        target = request.get("targetDate")
    else:
        target = None

    if target:
        return datetime.strptime(target, "%Y-%m-%d").date()
    return date.today() - timedelta(days=1)


def prepare_data(df_raw):
    logger.info("Preparing rows: %s", len(df_raw))
    if df_raw.empty:
        raise ValueError("No journey rows returned for target window")

    df = df_raw.copy()
    df["total_transactions"] = pd.to_numeric(df["total_transactions"], errors="coerce").fillna(0)
    df["total_transaction_value"] = pd.to_numeric(df["total_transaction_value"], errors="coerce").fillna(0)
    df["has_transaction"] = (df["total_transactions"] > 0).astype(int)
    df = df.sort_values(["unified_user_id", "transaction_id", "event_date"]).reset_index(drop=True)
    return df


def compact_path(path):
    compacted = []
    for channel in path:
        if pd.isna(channel):
            continue
        if not compacted or compacted[-1] != channel:
            compacted.append(channel)
    return compacted


def create_transaction_paths(df):
    user_paths = df.groupby(["unified_user_id", "transaction_id"], as_index=False).agg(
        {
            "channel": lambda x: compact_path(list(x)),
            "has_transaction": "max",
            "total_transaction_value": "sum",
        }
    )
    user_paths.columns = ["user_id", "transaction_id", "path", "converted", "value"]
    user_paths = user_paths[user_paths["path"].apply(lambda x: len(x) > 0)].reset_index(drop=True)
    logger.info("Transaction paths: %s, converted paths: %s", len(user_paths), user_paths["converted"].sum())
    return user_paths


def calculate_conversion_probability_safe(trans_matrix):
    try:
        if "Conversion" not in trans_matrix.columns or "Null" not in trans_matrix.columns:
            return 0.0

        transient_states = [s for s in trans_matrix.index if s not in ["Conversion", "Null"]]
        if "Start" not in transient_states:
            return 0.0

        q_matrix = trans_matrix.loc[transient_states, transient_states].values.astype(float)
        r_matrix = trans_matrix.loc[transient_states, ["Conversion", "Null"]].values.astype(float)
        identity = np.eye(len(transient_states))
        i_minus_q = identity - (q_matrix + Config.MARKOV_EPSILON * identity)

        if np.linalg.cond(i_minus_q) > Config.MARKOV_CONDITION_THRESHOLD:
            return 0.0

        try:
            fundamental = np.linalg.inv(i_minus_q)
        except np.linalg.LinAlgError:
            fundamental = np.linalg.pinv(i_minus_q)

        absorption = np.dot(fundamental, r_matrix)
        start_idx = transient_states.index("Start")
        probability = absorption[start_idx, 0]
        if np.isnan(probability) or np.isinf(probability):
            return 0.0
        return float(max(0.0, min(1.0, probability)))
    except Exception:
        logger.exception("Failed to calculate conversion probability")
        return 0.0


def markov_attribution(user_paths):
    if user_paths["converted"].sum() == 0:
        raise ValueError("No converted paths in the target window")

    transitions = defaultdict(lambda: defaultdict(int))
    for _, row in user_paths.iterrows():
        path = ["Start"] + list(row["path"])
        path.append("Conversion" if row["converted"] > 0 else "Null")
        for i in range(len(path) - 1):
            transitions[path[i]][path[i + 1]] += 1

    all_states = {"Start", "Conversion", "Null"}
    for state in transitions:
        all_states.add(state)
        all_states.update(transitions[state].keys())

    states = sorted(all_states)
    trans_matrix = pd.DataFrame(0.0, index=states, columns=states)

    for state in transitions:
        total = sum(transitions[state].values())
        if total > 0:
            for next_state, count in transitions[state].items():
                trans_matrix.loc[state, next_state] = count / total

    trans_matrix.loc["Conversion", "Conversion"] = 1.0
    trans_matrix.loc["Null", "Null"] = 1.0

    channels = [s for s in states if s not in ["Start", "Conversion", "Null"]]
    base_prob = calculate_conversion_probability_safe(trans_matrix)
    if base_prob <= 0:
        raise ValueError(f"Base conversion probability is {base_prob}")

    removal_effects = {}
    for channel in channels:
        mod_matrix = trans_matrix.copy()
        mod_matrix.loc[channel, :] = 0.0
        mod_matrix.loc[channel, "Null"] = 1.0
        new_prob = calculate_conversion_probability_safe(mod_matrix)
        removal_effects[channel] = max(0.0, (base_prob - new_prob) / base_prob)

    total_effect = sum(removal_effects.values())
    if total_effect == 0:
        weights = {channel: 1.0 / len(removal_effects) for channel in removal_effects}
    else:
        weights = {channel: value / total_effect for channel, value in removal_effects.items()}

    return pd.DataFrame(
        {
            "channel": list(weights.keys()),
            "Removal_Effect": [removal_effects[channel] for channel in weights.keys()],
            "Weight": [weights[channel] for channel in weights.keys()],
        }
    )


def ensure_daily_weight_table(client):
    query = f"""
    CREATE TABLE IF NOT EXISTS `{Config.DAILY_WEIGHT_TABLE}` (
      weight_date DATE NOT NULL,
      window_start_date DATE NOT NULL,
      window_end_date DATE NOT NULL,
      channel STRING NOT NULL,
      removal_effect FLOAT64,
      weight FLOAT64,
      run_timestamp TIMESTAMP NOT NULL
    )
    PARTITION BY weight_date
    CLUSTER BY channel
    """
    client.query(query, location=Config.BQ_LOCATION).result()


def load_source_data(client, target_date):
    window_start_date = target_date - timedelta(days=29)
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("window_start_date", "DATE", window_start_date.isoformat()),
            bigquery.ScalarQueryParameter("target_date", "DATE", target_date.isoformat()),
        ]
    )
    query = ATTRIBUTION_QUERY.format(source_events_table=Config.SOURCE_EVENTS_TABLE)
    df_raw = client.query(query, job_config=job_config, location=Config.BQ_LOCATION).result().to_dataframe()
    return window_start_date, df_raw


def merge_daily_weights(client, df_weights, target_date, window_start_date):
    if df_weights.empty:
        raise ValueError("Markov result is empty")

    run_timestamp = datetime.now(timezone.utc)
    rows = []
    for _, row in df_weights.iterrows():
        rows.append(
            "SELECT DATE '{weight_date}' AS weight_date, DATE '{window_start}' AS window_start_date, "
            "DATE '{window_end}' AS window_end_date, '{channel}' AS channel, "
            "{removal_effect} AS removal_effect, {weight} AS weight, TIMESTAMP('{run_timestamp}') AS run_timestamp".format(
                weight_date=target_date.isoformat(),
                window_start=window_start_date.isoformat(),
                window_end=target_date.isoformat(),
                channel=str(row["channel"]).replace("'", "''"),
                removal_effect=float(row["Removal_Effect"]),
                weight=float(row["Weight"]),
                run_timestamp=run_timestamp.isoformat(),
            )
        )

    query = f"""
    MERGE `{Config.DAILY_WEIGHT_TABLE}` AS target
    USING ({' UNION ALL '.join(rows)}) AS source
    ON target.weight_date = source.weight_date
      AND target.channel = source.channel
    WHEN MATCHED THEN UPDATE SET
      window_start_date = source.window_start_date,
      window_end_date = source.window_end_date,
      removal_effect = source.removal_effect,
      weight = source.weight,
      run_timestamp = source.run_timestamp
    WHEN NOT MATCHED THEN INSERT (
      weight_date, window_start_date, window_end_date, channel, removal_effect, weight, run_timestamp
    ) VALUES (
      source.weight_date, source.window_start_date, source.window_end_date, source.channel,
      source.removal_effect, source.weight, source.run_timestamp
    )
    """
    client.query(query, location=Config.BQ_LOCATION).result()


def upload_latest_debug_table(client, df_weights):
    df_upload = df_weights[["channel", "Removal_Effect", "Weight"]].copy()
    df_upload["run_timestamp"] = datetime.now(timezone.utc)
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE", autodetect=True)
    client.load_table_from_dataframe(df_upload, Config.LATEST_DETAILS_TABLE, job_config=job_config).result()


def trigger_dataform_workflow():
    if not Config.TRIGGER_DATAFORM_AFTER:
        return {"success": False, "skipped": True}

    client = dataform_v1beta1.DataformClient()
    parent = (
        f"projects/{Config.PROJECT_ID}/locations/{Config.DATAFORM_REGION}/"
        f"repositories/{Config.DATAFORM_REPOSITORY_ID}"
    )
    workflow_config_name = f"{parent}/workflowConfigs/{Config.DATAFORM_WORKFLOW_CONFIG_ID}"
    invocation = dataform_v1beta1.WorkflowInvocation(workflow_config=workflow_config_name)
    request = dataform_v1beta1.CreateWorkflowInvocationRequest(
        parent=parent,
        workflow_invocation=invocation,
    )
    try:
        response = client.create_workflow_invocation(request=request)
        return {"success": True, "invocation_name": response.name}
    except google_exceptions.FailedPrecondition as exc:
        message = str(exc)
        if "already an active execution" in message:
            logger.warning("Dataform workflow already has an active execution; skipping trigger.")
            return {"success": False, "skipped": True, "reason": "active_execution", "message": message}
        raise


def run_attribution_analysis(request):
    target_date = parse_target_date(request)
    client = bigquery.Client(project=Config.PROJECT_ID)

    ensure_daily_weight_table(client)
    window_start_date, df_raw = load_source_data(client, target_date)
    df = prepare_data(df_raw)
    user_paths = create_transaction_paths(df)
    df_weights = markov_attribution(user_paths)

    merge_daily_weights(client, df_weights, target_date, window_start_date)
    upload_latest_debug_table(client, df_weights)
    dataform_result = trigger_dataform_workflow()

    return {
        "status": "success",
        "target_date": target_date.isoformat(),
        "window_start_date": window_start_date.isoformat(),
        "rows": len(df_raw),
        "paths": len(user_paths),
        "channels": len(df_weights),
        "weight_sum": float(df_weights["Weight"].sum()),
        "dataform": dataform_result,
    }


def attribution_analysis(request):
    try:
        result = run_attribution_analysis(request)
        return json.dumps(result), 200
    except Exception as exc:
        logger.error("Execution failed: %s", exc)
        traceback.print_exc()
        return json.dumps({"status": "error", "message": str(exc)}), 500


def parse_backfill_request(request):
    if hasattr(request, "get_json"):
        payload = request.get_json(silent=True) or {}
        args = request.args or {}
        start = args.get("startDate") or payload.get("startDate")
        end = args.get("endDate") or payload.get("endDate")
        max_days = args.get("maxDays") or payload.get("maxDays")
        trigger_after = args.get("triggerDataformAfter") or payload.get("triggerDataformAfter")
    elif isinstance(request, dict):
        start = request.get("startDate")
        end = request.get("endDate")
        max_days = request.get("maxDays")
        trigger_after = request.get("triggerDataformAfter")
    else:
        start = end = max_days = trigger_after = None

    if not start or not end:
        raise ValueError("startDate and endDate are required, format YYYY-MM-DD")

    max_days_value = Config.BACKFILL_MAX_DAYS if max_days is None else int(max_days)
    trigger_after_value = str(trigger_after).lower() == "true"
    return (
        datetime.strptime(start, "%Y-%m-%d").date(),
        datetime.strptime(end, "%Y-%m-%d").date(),
        max_days_value,
        trigger_after_value,
    )


def iter_dates(start_date, end_date, max_days):
    current = start_date
    processed = 0
    while current <= end_date and (max_days <= 0 or processed < max_days):
        yield current
        current += timedelta(days=1)
        processed += 1


def backfill_markov(request):
    try:
        start_date, end_date, max_days, trigger_after = parse_backfill_request(request)
        results = []
        last_processed = None

        original_trigger = Config.TRIGGER_DATAFORM_AFTER
        Config.TRIGGER_DATAFORM_AFTER = False
        try:
            for target_date in iter_dates(start_date, end_date, max_days):
                result = run_attribution_analysis({"targetDate": target_date.isoformat()})
                results.append(result)
                last_processed = target_date
        finally:
            Config.TRIGGER_DATAFORM_AFTER = original_trigger

        next_start_date = None
        complete = True
        if last_processed and last_processed < end_date:
            next_start_date = (last_processed + timedelta(days=1)).isoformat()
            complete = False

        dataform_result = {"success": False, "skipped": True}
        if trigger_after and complete:
            Config.TRIGGER_DATAFORM_AFTER = True
            try:
                dataform_result = trigger_dataform_workflow()
            finally:
                Config.TRIGGER_DATAFORM_AFTER = original_trigger

        return json.dumps(
            {
                "status": "success",
                "startDate": start_date.isoformat(),
                "endDate": end_date.isoformat(),
                "processedDays": len(results),
                "complete": complete,
                "nextStartDate": next_start_date,
                "results": results,
                "dataform": dataform_result,
            }
        ), 200
    except Exception as exc:
        logger.error("Backfill failed: %s", exc)
        traceback.print_exc()
        return json.dumps({"status": "error", "message": str(exc)}), 500


def first_day_of_month(value):
    return date(value.year, value.month, 1)


def last_day_of_month(value):
    if value.month == 12:
        return date(value.year + 1, 1, 1) - timedelta(days=1)
    return date(value.year, value.month + 1, 1) - timedelta(days=1)


def add_month(value):
    if value.month == 12:
        return date(value.year + 1, 1, 1)
    return date(value.year, value.month + 1, 1)


def iter_months(start_month, end_month, max_months):
    current = first_day_of_month(start_month)
    end = first_day_of_month(end_month)
    processed = 0
    while current <= end and (max_months <= 0 or processed < max_months):
        yield current
        current = add_month(current)
        processed += 1


def iter_date_range(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def chunk_values(values, size):
    for index in range(0, len(values), size):
        yield values[index:index + size]


def fetch_report_min_date(client):
    query = f"SELECT MIN(date) AS min_date FROM `{Config.PROJECT_ID}.attribution.channel_performance`"
    rows = list(client.query(query, location=Config.BQ_LOCATION).result())
    return rows[0]["min_date"]


def load_source_data_for_window(client, window_start_date, window_end_date):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("window_start_date", "DATE", window_start_date.isoformat()),
            bigquery.ScalarQueryParameter("target_date", "DATE", window_end_date.isoformat()),
        ]
    )
    query = ATTRIBUTION_QUERY.format(source_events_table=Config.SOURCE_EVENTS_TABLE)
    df_raw = client.query(query, job_config=job_config, location=Config.BQ_LOCATION).result().to_dataframe()
    df = prepare_data(df_raw)
    return create_transaction_paths(df), len(df_raw)


def merge_weights_for_dates(client, df_weights, weight_dates, window_start_date, window_end_date):
    if df_weights.empty or not weight_dates:
        return

    run_timestamp = datetime.now(timezone.utc).isoformat()
    rows = []
    for weight_date in weight_dates:
        for _, row in df_weights.iterrows():
            rows.append(
                "SELECT DATE '{weight_date}' AS weight_date, DATE '{window_start}' AS window_start_date, "
                "DATE '{window_end}' AS window_end_date, '{channel}' AS channel, "
                "{removal_effect} AS removal_effect, {weight} AS weight, TIMESTAMP('{run_timestamp}') AS run_timestamp".format(
                    weight_date=weight_date.isoformat(),
                    window_start=window_start_date.isoformat(),
                    window_end=window_end_date.isoformat(),
                    channel=str(row["channel"]).replace("'", "''"),
                    removal_effect=float(row["Removal_Effect"]),
                    weight=float(row["Weight"]),
                    run_timestamp=run_timestamp,
                )
            )

    for rows_chunk in chunk_values(rows, 3000):
        query = f"""
        MERGE `{Config.DAILY_WEIGHT_TABLE}` AS target
        USING ({' UNION ALL '.join(rows_chunk)}) AS source
        ON target.weight_date = source.weight_date
          AND target.channel = source.channel
        WHEN MATCHED THEN UPDATE SET
          window_start_date = source.window_start_date,
          window_end_date = source.window_end_date,
          removal_effect = source.removal_effect,
          weight = source.weight,
          run_timestamp = source.run_timestamp
        WHEN NOT MATCHED THEN INSERT (
          weight_date, window_start_date, window_end_date, channel, removal_effect, weight, run_timestamp
        ) VALUES (
          source.weight_date, source.window_start_date, source.window_end_date, source.channel,
          source.removal_effect, source.weight, source.run_timestamp
        )
        """
        client.query(query, location=Config.BQ_LOCATION).result()


def parse_monthly_backfill_request(request):
    if hasattr(request, "get_json"):
        payload = request.get_json(silent=True) or {}
        args = request.args or {}
        monthly_start = args.get("monthlyStart") or payload.get("monthlyStart") or "2025-09-01"
        monthly_end = args.get("monthlyEnd") or payload.get("monthlyEnd") or "2026-05-31"
        fallback_start = args.get("fallbackStart") or payload.get("fallbackStart")
        max_months = args.get("maxMonths") or payload.get("maxMonths")
        trigger_after = args.get("triggerDataformAfter") or payload.get("triggerDataformAfter")
    elif isinstance(request, dict):
        monthly_start = request.get("monthlyStart") or "2025-09-01"
        monthly_end = request.get("monthlyEnd") or "2026-05-31"
        fallback_start = request.get("fallbackStart")
        max_months = request.get("maxMonths")
        trigger_after = request.get("triggerDataformAfter")
    else:
        monthly_start, monthly_end, fallback_start, max_months, trigger_after = "2025-09-01", "2026-05-31", None, None, None

    return (
        datetime.strptime(monthly_start, "%Y-%m-%d").date(),
        datetime.strptime(monthly_end, "%Y-%m-%d").date(),
        datetime.strptime(fallback_start, "%Y-%m-%d").date() if fallback_start else None,
        Config.BACKFILL_MAX_MONTHS if max_months is None else int(max_months),
        str(trigger_after).lower() == "true",
    )


def backfill_monthly_markov(request):
    try:
        monthly_start, monthly_end, fallback_start, max_months, trigger_after = parse_monthly_backfill_request(request)
        client = bigquery.Client(project=Config.PROJECT_ID)
        ensure_daily_weight_table(client)

        report_min_date = fallback_start or fetch_report_min_date(client)
        results = []
        cached_first_month = None
        last_processed_month = None

        for month_start in iter_months(monthly_start, monthly_end, max_months):
            month_end = min(last_day_of_month(month_start), monthly_end)
            user_paths, raw_rows = load_source_data_for_window(client, month_start, month_end)
            df_weights = markov_attribution(user_paths)
            weight_dates = list(iter_date_range(month_start, month_end))
            merge_weights_for_dates(client, df_weights, weight_dates, month_start, month_end)

            if month_start == first_day_of_month(monthly_start):
                cached_first_month = (df_weights, month_start, month_end)

            last_processed_month = month_start
            results.append(
                {
                    "monthStart": month_start.isoformat(),
                    "monthEnd": month_end.isoformat(),
                    "rawRows": raw_rows,
                    "paths": len(user_paths),
                    "channels": len(df_weights),
                    "datesWritten": len(weight_dates),
                    "weightSum": float(df_weights["Weight"].sum()),
                }
            )

        next_month_start = None
        complete = True
        if last_processed_month and add_month(last_processed_month) <= first_day_of_month(monthly_end):
            next_month_start = add_month(last_processed_month).isoformat()
            complete = False

        fallback_result = None
        if report_min_date < monthly_start and cached_first_month is not None:
            df_weights, source_start, source_end = cached_first_month
            fallback_end = monthly_start - timedelta(days=1)
            fallback_dates = list(iter_date_range(report_min_date, fallback_end))
            merge_weights_for_dates(client, df_weights, fallback_dates, source_start, source_end)
            fallback_result = {
                "fallbackStart": report_min_date.isoformat(),
                "fallbackEnd": fallback_end.isoformat(),
                "sourceMonthStart": source_start.isoformat(),
                "sourceMonthEnd": source_end.isoformat(),
                "datesWritten": len(fallback_dates),
                "channels": len(df_weights),
            }

        dataform_result = {"success": False, "skipped": True}
        if trigger_after and complete:
            dataform_result = trigger_dataform_workflow()

        return json.dumps(
            {
                "status": "success",
                "monthlyStart": monthly_start.isoformat(),
                "monthlyEnd": monthly_end.isoformat(),
                "processedMonths": len(results),
                "complete": complete,
                "nextMonthlyStart": next_month_start,
                "results": results,
                "fallback": fallback_result,
                "dataform": dataform_result,
            }
        ), 200
    except Exception as exc:
        logger.error("Monthly backfill failed: %s", exc)
        traceback.print_exc()
        return json.dumps({"status": "error", "message": str(exc)}), 500
