import argparse
import importlib.util
import os
from datetime import datetime, timedelta
from pathlib import Path


def date_range(start_date, end_date):
    current = start_date
    while current <= end_date:
        yield current
        current += timedelta(days=1)


def load_markov_function(repo_root):
    function_path = repo_root / "cloud-function" / "markov-python" / "main.py"
    spec = importlib.util.spec_from_file_location("markov_function", function_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    parser = argparse.ArgumentParser(
        description="Backfill daily Markov weights by reusing the Cloud Function code."
    )
    parser.add_argument("--start", required=True, help="Start date, YYYY-MM-DD")
    parser.add_argument("--end", required=True, help="End date, YYYY-MM-DD")
    parser.add_argument(
        "--keyfile",
        help="Optional service account JSON path. Sets GOOGLE_APPLICATION_CREDENTIALS.",
    )
    parser.add_argument(
        "--project",
        default="bigquery-2024",
        help="GCP project id. Defaults to bigquery-2024.",
    )
    args = parser.parse_args()

    if args.keyfile:
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = args.keyfile

    os.environ["PROJECT_ID"] = args.project
    os.environ["TRIGGER_DATAFORM_AFTER"] = "false"

    repo_root = Path(__file__).resolve().parents[1]
    markov_function = load_markov_function(repo_root)

    start_date = datetime.strptime(args.start, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end, "%Y-%m-%d").date()

    for target_date in date_range(start_date, end_date):
        target = target_date.isoformat()
        print(f"Backfilling Markov weights for {target}...")
        result = markov_function.run_attribution_analysis({"targetDate": target})
        print(result)

    print("Backfill complete. Run Dataform downstream actions once after this finishes.")


if __name__ == "__main__":
    main()

