# Google Cloud setup (step by step)

You do this once. Allow about 30 minutes. The Spark job runs on **Dataproc Serverless** in Google Cloud, so you install **no Java, no Spark and no cluster**. On your computer you need only Python and the Google Cloud CLI (`gcloud`). The pipeline handles very little data, so costs are tiny, but always set a budget alert (step 2).

## 1. Create a project
1. Go to console.cloud.google.com and sign in.
2. Click the project picker at the top, then **New project**. Name it, for example, `air-quality-pipeline`.
3. Note the **Project ID** (it is shown under the name and is usually not the same as the name). You will need it later.

## 2. Set a budget alert (do this first)
1. Open **Billing** in the left menu and link a billing account to the project. Dataproc Serverless and Cloud Storage both need it (the BigQuery sandbox alone is not enough for this project).
2. Go to **Budgets & alerts**, click **Create budget**, set a very small amount, and keep the email alerts on.

## 3. Install the Google Cloud CLI and log in
1. Install it from cloud.google.com/sdk/docs/install and open a **new** terminal. Check it with `gcloud --version`.
2. Run:
```bash
gcloud auth login
gcloud config set project YOUR_PROJECT_ID
gcloud auth application-default login
```
`gcloud auth login` is used by the `gcloud` commands (the pipeline calls `gcloud` to start the Dataproc job). `gcloud auth application-default login` saves credentials for the Python libraries (Cloud Storage and BigQuery). **You never put a key file in your project.**

## 4. Enable the APIs
```bash
gcloud services enable dataproc.googleapis.com compute.googleapis.com bigquery.googleapis.com storage.googleapis.com
```
Or in the console: **APIs & Services -> Library**, then search for and enable the **Dataproc API** (it runs Spark), the **Compute Engine API** (the network Dataproc uses), the **BigQuery API** and **Cloud Storage**. If you created the project yourself you are its Owner, so you already have the permissions you need to start a job.

## 5. Create a Cloud Storage bucket
Dataproc cannot see files on your computer, so the pipeline copies the raw file and the job code into this bucket, and the job reads and writes there. Bucket names must be globally unique, for example `yourname-air-quality-lake-123`.
```bash
gcloud storage buckets create gs://YOUR_BUCKET_NAME --location=US
```
You can also do this in the console under **Cloud Storage -> Buckets -> Create**. Keep the location the same as `BQ_LOCATION` (default `US`).

## 6. Prepare the network and the permissions for Dataproc
Two things must be right before the first job, or it fails while starting.

**a) Private Google Access on the network.** A Dataproc Serverless job runs on workers without public addresses, so the subnet must allow them to reach Google services (Cloud Storage, BigQuery). For the `default` network in your region:
```bash
gcloud compute networks subnets update default --region=us-central1 --enable-private-ip-google-access
```
Use your own region if you changed `DATAPROC_REGION`.

**b) The job's service account must reach your bucket.** The job runs as the project's *default Compute Engine service account*, `PROJECT_NUMBER-compute@developer.gserviceaccount.com`. Get your project number with:
```bash
gcloud projects describe YOUR_PROJECT_ID --format="value(projectNumber)"
```
Then give that account two roles:
```bash
gcloud projects add-iam-policy-binding YOUR_PROJECT_ID --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" --role="roles/dataproc.worker"
gcloud storage buckets add-iam-policy-binding gs://YOUR_BUCKET_NAME --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" --role="roles/storage.objectAdmin"
```
In the console this is **IAM & Admin -> IAM**, find that account (tick "Include Google-provided role grants" if it is not listed) and add **Dataproc Worker**, and on the bucket (**Permissions** tab) add **Storage Object Admin**.

**c) Your own account.** As the project Owner you can already submit jobs. If you use another account, it needs **Dataproc Editor** and **Service Account User** on the project.

If your organization blocks the `default` network or its default service account, ask your administrator or use your own network and service account. That is outside this guide.

## 7. Configure the project
```bash
copy .env.example .env      # Mac/Linux: cp .env.example .env
```
Open `.env` and fill in:
```
GCP_PROJECT_ID=your-project-id
BQ_DATASET=air_quality
BQ_TABLE=daily_air_quality
BQ_LOCATION=US
GCS_BUCKET=your-bucket-name           # required: the raw data, the job code and the results live here
DATAPROC_REGION=us-central1           # the region where Google runs the job
DATAPROC_RUNTIME_VERSION=2.3          # the Dataproc Serverless runtime (a Spark version)
```

## 8. Run it
You need Python 3.10 or newer. In the project folder:
```bash
python -m venv venv
venv\Scripts\activate            # Mac/Linux: source venv/bin/activate
pip install -r requirements.txt
python run_pipeline.py
python scripts/compare_with_expected.py
```
Expected, step by step:
- `[1/4] Upload` copies the raw file and the job code to the bucket.
- `[2/4] Dataproc` starts the batch. Nothing seems to happen for a minute or two while Google prepares Spark. Then the job's own output appears, ending with the data-quality report (4,445 raw rows in, 4,272 clean rows out for the file in the repo).
- `[3/4] Load` loads BigQuery. `[4/4] Download` saves `outputs/quality_report.json` and `outputs/daily_air_quality_clean.csv`.
- The last command prints `OK` when the output matches the expected output.

If a data-quality check fails, the job stops, nothing is loaded, and `outputs/quality_report.json` shows which check failed.

**Look at the job in the console.** Open **Dataproc -> Serverless -> Batches** (or search for "batches"). Every run is listed with its ID (`air-quality-DATE-TIME`), its status and its logs. This is the first place to look when something goes wrong. In a terminal:
```bash
gcloud dataproc batches list --region=us-central1
gcloud dataproc batches describe BATCH_ID --region=us-central1
```

**Cost.** Dataproc Serverless bills for the resources used while the batch runs, which is a few minutes for this data. There is no cluster that stays on. Cloud Storage and BigQuery cost almost nothing at this size. Check the Billing reports after your first run to see the real number.

## 9. Check the result in the console
1. Open **BigQuery**, then in the Explorer panel open your project -> `air_quality` -> `daily_air_quality`.
2. **Schema** tab: the 14 columns. **Details** tab: you should see *Partitioned by Day on reading_date* and *Clustered by city*. **Preview** tab: sample rows.
3. Click **Query** and run:
```sql
SELECT city, ROUND(AVG(pm25), 2) AS avg_pm25
FROM `YOUR_PROJECT_ID.air_quality.daily_air_quality`
GROUP BY city
ORDER BY avg_pm25 DESC;
```
4. Open your bucket: you should see `raw/`, `code/`, `processed/` and `reports/`.
5. Take screenshots of the Batches page, the table details and a query result for your README.

## 10. Clean up when you are done
```bash
bq rm -r -f -d YOUR_PROJECT_ID:air_quality
gcloud storage rm --recursive gs://YOUR_BUCKET_NAME
```
There is no cluster to delete. Or delete the whole project from **IAM & Admin -> Settings -> Shut down**.

## Common problems
| Error | Fix |
|---|---|
| `The gcloud command was not found` | Install it (step 3) and open a **new** terminal so the `gcloud` command is found |
| `API [dataproc.googleapis.com] not enabled` | Enable the Dataproc API (step 4) and wait a minute |
| `Permission ... dataproc.batches.create denied` or `iam.serviceAccounts.actAs` | Your account needs **Dataproc Editor** and **Service Account User** on the project (step 6c) |
| The batch fails at start with a message about the **subnet**, **network** or **Private Google Access** | Run the `gcloud compute networks subnets update ... --enable-private-ip-google-access` command in step 6a, then run again |
| The job starts, then fails reading or writing `gs://` (permission denied, 403) | The default Compute Engine service account lacks the roles in step 6b |
| `Batch ... already exists` | Each run gets a new ID from the date and time. Wait a second and run again |
| The job failed and you do not know why | Open Dataproc -> Serverless -> Batches, click the batch ID and read the logs. Or run `gcloud dataproc batches describe BATCH_ID --region=YOUR_REGION` |
| The job fails with `Data quality checks failed` | The data broke a rule. Read `outputs/quality_report.json` (the pipeline saves it even when the job fails) |
| Runtime version not supported | Dataproc retires old runtime versions. Pick a current one from the Dataproc Serverless release notes and set `DATAPROC_RUNTIME_VERSION` in `.env` |
| `DefaultCredentialsError` | Run `gcloud auth application-default login` again |
| `403 ... Access Denied` from BigQuery or Storage | Wrong project ID, or your account lacks permissions on it |
| `404 Not found: Dataset` or location errors | Keep `BQ_LOCATION` the same everywhere (bucket and dataset) |
| `Bucket does not exist` or `Set GCS_BUCKET` | Create the bucket (step 5) and set `GCS_BUCKET` in `.env` |
| Billing required | Link a billing account to the project (step 2) |
