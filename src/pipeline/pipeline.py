"""Orchestrates: extract -> transform -> quality gate -> load -> SQL analytics."""
import json
from pathlib import Path

from . import analytics, local_warehouse
from .config import Settings
from .extract import read_raw_csv
from .quality import assert_quality
from .transform import clean


def run(input_path: str, mode: str = "local", out_dir: str = "outputs", settings: Settings | None = None) -> dict:
    settings = settings or Settings.from_env()
    out = Path(out_dir)
    out.mkdir(parents=True, exist_ok=True)

    print(f"[1/5] Extract   reading {input_path}")
    raw = read_raw_csv(input_path)

    print("[2/5] Transform cleaning and enriching")
    df, report = clean(raw)
    processed_csv = out / "daily_air_quality_clean.csv"
    df.to_csv(processed_csv, index=False)

    print("[3/5] Quality   running data-quality checks")
    report["quality_checks"] = [{"check": n, "passed": ok} for n, ok in assert_quality(df)]
    (out / "quality_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    if mode == "local":
        print("[4/5] Load      loading into local SQLite (BigQuery stand-in)")
        conn = local_warehouse.load_dataframe(df, settings.bq_table)
        runner = lambda sql: local_warehouse.run_query(conn, sql)  # noqa: E731
        table_ref = settings.bq_table
    elif mode == "gcp":
        from . import load_gcp

        if not settings.project_id:
            raise ValueError("Set GCP_PROJECT_ID (see .env.example) before running with --mode gcp")
        gcs_uri = None
        if settings.gcs_bucket:
            print(f"[4/5] Load      uploading to gs://{settings.gcs_bucket} and loading into BigQuery")
            gcs_uri = load_gcp.upload_to_gcs(str(processed_csv), settings.gcs_bucket,
                                             "processed/daily_air_quality_clean.csv", settings.project_id)
            load_gcp.upload_to_gcs(input_path, settings.gcs_bucket, "raw/" + Path(input_path).name, settings.project_id)
        else:
            print("[4/5] Load      loading into BigQuery (no bucket configured, skipping Cloud Storage)")
        rows = load_gcp.load_to_bigquery(str(processed_csv), settings.project_id, settings.bq_dataset,
                                         settings.bq_table, settings.bq_location, gcs_uri)
        report["bigquery_rows"] = int(rows)
        runner = lambda sql: load_gcp.run_bigquery_query(sql, settings.project_id, settings.bq_location)  # noqa: E731
        table_ref = f"`{settings.table_id}`"
    else:
        raise ValueError("mode must be 'local' or 'gcp'")

    print("[5/5] Analyse   running SQL queries")
    results = analytics.run_all(runner, table_ref, str(out / "query_results"))
    report["queries_run"] = list(results)
    return {"report": report, "results": results}
