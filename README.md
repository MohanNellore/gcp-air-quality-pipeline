# GCP Air-Quality Batch Data Pipeline

A beginner-friendly **data engineering project on Google Cloud**. It takes a messy raw CSV of daily city air-quality readings, cleans and validates it with **PySpark** running as a **Dataproc Serverless batch job**, stages it in **Cloud Storage**, and loads it into a **partitioned and clustered BigQuery table**.

All the code is Python. You start the whole pipeline with one command from your own computer. The Spark job runs on Dataproc in Google Cloud, which already contains Spark and Java, so you install **no Java, no Spark and no cluster**.

## What it does

```
your computer:  python run_pipeline.py
      │
 [1] Upload ─── raw CSV + job code  ──>  Cloud Storage (raw/, code/)
      │
 [2] Dataproc ─ one Serverless batch job, written in PySpark, runs in Google Cloud:
      │           Extract    read the raw CSV from Cloud Storage
      │           Transform  fix city names, drop invalid dates and duplicate city-days,
      │                      null out negative values, drop rows with no measurements,
      │                      derive missing AQI from PM2.5, add AQI bucket, year, month, season
      │           Quality    stop the job if the cleaned data breaks a rule
      │           Write      clean CSV  ──>  Cloud Storage (processed/), report ──> reports/
      │
 [3] Load ───── Cloud Storage  ──>  BigQuery table, partitioned by reading_date, clustered by city
      │
 [4] Download ─ quality_report.json and the clean CSV  ──>  outputs/ on your computer
```

## Skills shown
PySpark ETL on Dataproc Serverless (DataFrames, window functions, lazy evaluation) · submitting and monitoring a Spark batch job · data cleaning and validation · data-quality gate · Google Cloud Storage as a data lake · BigQuery (partitioning, clustering, explicit schema, idempotent loads) · configuration through environment variables · unit tests · Git

## Project structure
```
├── run_pipeline.py              # the one command you run on your computer
├── jobs/air_quality_job.py      # the file Dataproc starts (reads its settings from arguments)
├── requirements.txt             # Google libraries only. No Spark and no Java on your computer
├── src/pipeline/
│   ├── pipeline.py              # the manager (your computer): upload -> Dataproc -> BigQuery -> download
│   ├── dataproc.py              # packs the code into a zip and submits the batch with gcloud
│   ├── job.py                   # the Spark job (Dataproc): extract, transform, quality gate, write
│   ├── spark_utils.py           # start Spark, write a DataFrame as one CSV
│   ├── rules.py                 # schema, lookup tables, plain-Python helper rules
│   ├── extract.py               # read the raw CSV with Spark
│   ├── transform.py             # cleaning rules + quality counters (PySpark)
│   ├── quality.py               # data-quality gate (PySpark)
│   ├── load_gcp.py              # Cloud Storage upload/download + BigQuery load
│   └── config.py                # settings from environment variables
├── scripts/generate_sample_data.py   # plain Python (no Spark needed)
├── scripts/compare_with_expected.py  # checks the output against the expected output
├── data/raw/air_quality_raw.csv # SYNTHETIC sample data with deliberate problems
├── tests/                       # unit tests (+ expected/ = the expected clean output to compare with)
└── docs/                        # GCP_SETUP.md, LEARNING_GUIDE.md
```

Two programs work together. The **manager** (`pipeline.py`, `dataproc.py`) runs on your computer and only uses Python and the Google Cloud CLI. The **Spark job** (`jobs/air_quality_job.py` and `job.py`) runs on Dataproc. Dataproc cannot see files on your computer, so the manager first copies the raw file and the job code into your bucket.

## Run it

You need a Google Cloud project with billing turned on, the Google Cloud CLI (`gcloud`) and Python 3.10 or newer. A run costs very little: the batch lives for a few minutes and then Google removes it.

**1. One-time setup** (login, APIs, bucket, network and permissions, `.env` file): follow [docs/GCP_SETUP.md](docs/GCP_SETUP.md).

**2. Run it**
```bash
git clone https://github.com/MohanNellore/gcp-air-quality-pipeline.git
cd gcp-air-quality-pipeline
python -m venv venv
venv\Scripts\activate            # Mac/Linux: source venv/bin/activate
pip install -r requirements.txt
python run_pipeline.py
python scripts/compare_with_expected.py
```
What happens:
1. The raw CSV and the job code are copied to your bucket (`raw/` and `code/` folders).
2. A Dataproc Serverless batch starts. Its Spark job reads the CSV from the bucket, cleans it and runs the quality checks. The job output appears in your terminal. Starting takes a minute or two.
3. If every check passes, the job writes the clean CSV to `processed/` and BigQuery loads it from there. If a check fails, the job stops, the load is skipped, and the report (written to `reports/` either way) is saved in `outputs/` so you can see which check failed.
4. A copy of the clean CSV and `quality_report.json` are saved in `outputs/` on your computer.

`compare_with_expected.py` checks that the output matches the expected results saved in `tests/expected/`, cell by cell. Open the BigQuery console to see the `air_quality.daily_air_quality` table, partitioned by `reading_date` and clustered by `city`. Every batch is also listed in the console under Dataproc -> Serverless -> Batches, with its logs.

### Tests
```bash
python -m unittest discover -s tests -v
```
The tests cover the plain-Python rules, the Dataproc command and zip, and the pipeline order and failure handling. They need no Spark, no Java and no Google account. The Spark cleaning code is checked by the real Dataproc run followed by `python scripts/compare_with_expected.py`, which compares every output cell with the expected output.

### Screenshots from my Google Cloud run
I ran the Spark job by submitting it by hand in the Google Cloud console (Dataproc -> Jobs -> Serverless Spark), with the same files and arguments that `run_pipeline.py` uses, and then loaded BigQuery from the console.

**Dataproc Serverless job** (PySpark, succeeded in under 2 minutes, about 0.2 DCU-hours):

![Dataproc Serverless job succeeded](images/dataproc-job-succeeded.png)

**Cloud Storage bucket (data lake)** with the `raw/`, `code/`, `processed/` and `reports/` folders:

![Cloud Storage bucket with raw, code, processed and reports folders](images/gcs-bucket.png)

**BigQuery table**: partitioned by day on `reading_date` and clustered by `city`. It holds 4,272 rows, the expected count:

![BigQuery table details](images/bigquery-table-details.png)

## About the data
`data/raw/air_quality_raw.csv` is **synthetic** (generated by `scripts/generate_sample_data.py`, plain Python) so the project is self-contained. The file in the repo came from an earlier version of the script; running the script again gives a similar file with the same kinds of problems, but not the same rows. It mimics a public city-level air-quality layout (`City, Date, PM2.5, PM10, NO2, SO2, CO, O3, AQI, AQI_Bucket`) and includes deliberate problems: missing values, negative readings, duplicate rows, messy city names and invalid dates. To use real data, replace the CSV with a file that has the same columns. Dates must be `YYYY-MM-DD`.

The AQI derivation from PM2.5 uses simplified breakpoints modelled on India's CPCB scale. It is for learning, not for official reporting.

## Design decisions
| Decision | Reason |
|---|---|
| Dataproc Serverless batch | No cluster to create, size or delete. Google starts Spark for the job, runs it and removes it, and you pay only while it runs |
| One command from your computer | The manager uploads, submits the job, waits for it, loads BigQuery and downloads the results. Simple to run and to explain |
| Code goes to the bucket as a zip (`--py-files`) | Dataproc cannot see files on your computer. The file Dataproc starts is `jobs/air_quality_job.py`, and the rest of the code travels as `pipeline.zip` |
| Files go through Cloud Storage | The job reads `raw/` and writes `processed/` and `reports/` in the bucket. This is also how a real data lake works |
| Pinned runtime version (`DATAPROC_RUNTIME_VERSION`) | The Spark version stays the same from run to run. Change it in `.env` when needed |
| PySpark for every transformation | The same code can handle much bigger data when Dataproc gives the job more workers |
| Read all columns as text, then cast | Spark does not guess types wrong, and every bad value is counted by a rule |
| Window function for duplicates | `row_number()` over (city, day) keeps the first row of each group, the Spark way to deduplicate |
| `cache()` on tables that are used again | Spark is lazy; caching stops it from repeating all the work for every count |
| One CSV file (`coalesce(1)`) | Easy to load into BigQuery and to download. Fine for small data; for big data keep Spark's parallel part files or Parquet |
| Quality gate before the clean data is written | Bad data never reaches `processed/` or the warehouse. The report is still written, so you can see what failed |
| Explicit BigQuery schema | No surprises from schema auto-detection |
| `WRITE_TRUNCATE` load | Re-running gives the same result, never duplicates (idempotent) |
| Partition by date, cluster by city | Cheaper, faster queries that filter by date or city |
| Every cleaning rule counts its changes | The report shows exactly what was fixed |
| Your login for the manager, a service account for the job | Credentials come from `gcloud auth application-default login`, so no keys are stored in the repo. The batch runs as the project's default Compute Engine service account |

## Limitations and next steps
- Batch only. Add incremental loads (append new days, then `MERGE`).
- CSV staging. Parquet (`df.write.parquet(...)` in Spark) would be smaller and keep types.
- The data is tiny. For big data, drop `coalesce(1)`, write Parquet and let Dataproc use more workers.
- Run on demand from your computer. To schedule it, trigger the same batch with Cloud Scheduler and Workflows, or orchestrate with Cloud Composer (Airflow).
- The raw file is copied to the bucket from your computer. In a real setup, files would land in the bucket from other systems and the pipeline would only read them.
- Add a dashboard (Looker Studio) on top of the BigQuery table.

## The setup in commands (Windows PowerShell or Mac/Linux)

The same steps as [docs/GCP_SETUP.md](docs/GCP_SETUP.md), as commands. Install the Google Cloud CLI first (cloud.google.com/sdk/docs/install). Replace the project ID, project number, region and bucket name with your own.

**1. Log in and choose the project**
```bash
gcloud auth login
gcloud config set project air-quality-pipeline-511610
gcloud auth application-default login
gcloud auth application-default set-quota-project air-quality-pipeline-511610
```

**2. Enable the APIs**
```bash
gcloud services enable dataproc.googleapis.com compute.googleapis.com bigquery.googleapis.com storage.googleapis.com
```

**3. Create the Cloud Storage bucket (data lake)**
```bash
gcloud storage buckets create gs://air-quality-pipeline-511610-lake --location=US
```

**4. Let Dataproc Serverless use the network and the bucket**
```bash
gcloud compute networks subnets update default --region=us-central1 --enable-private-ip-google-access
gcloud projects describe air-quality-pipeline-511610 --format="value(projectNumber)"
gcloud projects add-iam-policy-binding air-quality-pipeline-511610 --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" --role="roles/dataproc.worker"
gcloud storage buckets add-iam-policy-binding gs://air-quality-pipeline-511610-lake --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" --role="roles/storage.objectAdmin"
```
(The second command prints your project number. Use it in place of `PROJECT_NUMBER`.)

**5. Fill in the settings**: copy `.env.example` to `.env` and set `GCP_PROJECT_ID` and `GCS_BUCKET`. `DATAPROC_REGION` (default `us-central1`) and `DATAPROC_RUNTIME_VERSION` (default `2.3`) can stay as they are.

**6. Run the pipeline** (see "Run it" above)
```bash
python run_pipeline.py
```

**7. Check the result (optional)**
```bash
bq query --use_legacy_sql=false "SELECT city, ROUND(AVG(pm25), 2) AS avg_pm25 FROM `air-quality-pipeline-511610.air_quality.daily_air_quality` GROUP BY city ORDER BY avg_pm25 DESC"
```

**8. Clean up so nothing is billed**
```bash
bq rm -r -f -d air-quality-pipeline-511610:air_quality
gcloud storage rm --recursive gs://air-quality-pipeline-511610-lake
```

## License
MIT
