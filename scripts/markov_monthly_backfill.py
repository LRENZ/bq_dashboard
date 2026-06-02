import argparse
import importlib.util
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import pandas as pd
from google.cloud import bigquery


def load_markov_module(repo_root):
    function_path = repo_root / "cloud-function" / "markov-python" / "main.py"
    spec = importlib.util.spec_from_file_location("markov_function", function_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


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


def date_range(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def month_range(start_month, end_month):
    current = first_day_of_month(start_month)
    end = first_day_of_month(end_month)
    while current <= end:
        yield current
        current = add_month(current)


def sql_string(value):
    return str(value).replace("'", "''")


def chunk(values, size):
    for i in range(0, len(values), size):
        yield values[i:i + size]


def load_paths_for_window(markov_module, client, window_start, window_end):
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("window_start_date", "DATE", window_start.isoformat()),
            bigquery.ScalarQueryParameter("target_date", "DATE", window_end.isoformat()),
        ]
    )
    query = markov_module.ATTRIBUTION_QUERY.format(
        source_events_table=markov_module.Config.SOURCE_EVENTS_TABLE
    )
    df_raw = client.query(
        query,
        job_config=job_config,
        location=markov_module.Config.BQ_LOCATION,
    ).result().to_dataframe()
    df = markov_module.prepare_data(df_raw)
    return markov_module.create_transaction_paths(df), len(df_raw)


def merge_weights_for_dates(markov_module, client, df_weights, weight_dates, window_start, window_end):
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
                    window_start=window_start.isoformat(),
                    window_end=window_end.isoformat(),
                    channel=sql_string(row["channel"]),
                    removal_effect=float(row["Removal_Effect"]),
                    weight=float(row["Weight"]),
                    run_timestamp=run_timestamp,
                )
            )

    for row_chunk in chunk(rows, 3000):
        query = f"""
        MERGE `{markov_module.Config.DAILY_WEIGHT_TABLE}` AS target
        USING ({' UNION ALL '.join(row_chunk)}) AS source
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
        client.query(query, location=markov_module.Config.BQ_LOCATION).result()


def fetch_report_date_bounds(client, project_id):
    query = f"""
    SELECT MIN(date) AS min_date, MAX(date) AS max_date
    FROM `{project_id}.attribution.channel_performance`
    """
    rows = list(client.query(query, location="US").result())
    return rows[0]["min_date"], rows[0]["max_date"]


def main():
    parser = argparse.ArgumentParser(description="Monthly Markov historical backfill.")
    parser.add_argument("--monthly-start", default="2025-09-01")
    parser.add_argument("--monthly-end", default="2026-05-31")
    parser.add_argument("--fallback-start", help="Optional earliest date to fill. Defaults to channel_performance min(date).")
    parser.add_argument("--keyfile", help="Service account JSON path.")
    parser.add_argument("--project", default="bigquery-2024")
    args = parser.parse_args()

    if args.keyfile:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.keyfile
    os.environ["PROJECT_ID"] = args.project
    os.environ["TRIGGER_DATAFORM_AFTER"] = "false"

    repo_root = Path(__file__).resolve().parents[1]
    markov_module = load_markov_module(repo_root)
    client = bigquery.Client(project=args.project)
    markov_module.ensure_daily_weight_table(client)

    report_min_date, _ = fetch_report_date_bounds(client, args.project)
    fallback_start = (
        datetime.strptime(args.fallback_start, "%Y-%m-%d").date()
        if args.fallback_start
        else report_min_date
    )
    monthly_start = datetime.strptime(args.monthly_start, "%Y-%m-%d").date()
    monthly_end = datetime.strptime(args.monthly_end, "%Y-%m-%d").date()

    cached_weights = {}
    for month_start in month_range(monthly_start, monthly_end):
        window_start = month_start
        window_end = min(last_day_of_month(month_start), monthly_end)
        print(f"Calculating monthly weights for {window_start} to {window_end}...")
        user_paths, raw_rows = load_paths_for_window(markov_module, client, window_start, window_end)
        df_weights = markov_module.markov_attribution(user_paths)
        cached_weights[month_start] = (df_weights, window_start, window_end, raw_rows, len(user_paths))

        write_start = month_start
        write_end = window_end
        weight_dates = list(date_range(write_start, write_end))
        merge_weights_for_dates(markov_module, client, df_weights, weight_dates, window_start, window_end)
        print(
            f"{window_start}: raw_rows={raw_rows}, paths={len(user_paths)}, "
            f"channels={len(df_weights)}, dates_written={len(weight_dates)}, weight_sum={df_weights['Weight'].sum():.8f}"
        )

    if fallback_start < monthly_start:
        september_month = first_day_of_month(monthly_start)
        df_weights, window_start, window_end, raw_rows, path_count = cached_weights[september_month]
        fallback_end = monthly_start - timedelta(days=1)
        fallback_dates = list(date_range(fallback_start, fallback_end))
        print(
            f"Writing fallback dates {fallback_start} to {fallback_end} "
            f"using {window_start} to {window_end} weights..."
        )
        merge_weights_for_dates(markov_module, client, df_weights, fallback_dates, window_start, window_end)
        print(
            f"fallback: source_raw_rows={raw_rows}, source_paths={path_count}, "
            f"channels={len(df_weights)}, dates_written={len(fallback_dates)}"
        )

    print("Monthly Markov backfill complete.")


if __name__ == "__main__":
    main()

