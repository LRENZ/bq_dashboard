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


class Config:
    PROJECT_ID = os.environ.get("PROJECT_ID", "bigquery-2024")
    DEST_DATASET_ID = os.environ.get("DEST_DATASET_ID", "attribution_v1")
    SOURCE_JOURNEY_TABLE = os.environ.get(
        "SOURCE_JOURNEY_TABLE",
        "bigquery-2024.attribution.session_source_medium",
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

    # Dataform should usually run session_source_medium before this function,
    # then downstream attribution tables after this function.
    TRIGGER_DATAFORM_AFTER = os.environ.get("TRIGGER_DATAFORM_AFTER", "false").lower() == "true"
    DATAFORM_REGION = os.environ.get("DATAFORM_REGION", "us-central1")
    DATAFORM_REPOSITORY_ID = os.environ.get("DATAFORM_REPOSITORY_ID", "bq")
    DATAFORM_WORKFLOW_CONFIG_ID = os.environ.get("DATAFORM_WORKFLOW_CONFIG_ID", "ga4_attribution")

    MARKOV_EPSILON = 1e-10
    MARKOV_CONDITION_THRESHOLD = 1e10


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


ATTRIBUTION_QUERY = """
SELECT
  unified_user_id,
  transaction_id,
  event_date,
  channel,
  total_transactions,
  total_transaction_value,
  has_transaction
FROM `{source_journey_table}`
WHERE purchase_date BETWEEN @window_start_date AND @target_date
  AND transaction_id IS NOT NULL
  AND channel IS NOT NULL
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
    query = ATTRIBUTION_QUERY.format(source_journey_table=Config.SOURCE_JOURNEY_TABLE)
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
    response = client.create_workflow_invocation(request=request)
    return {"success": True, "invocation_name": response.name}


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

