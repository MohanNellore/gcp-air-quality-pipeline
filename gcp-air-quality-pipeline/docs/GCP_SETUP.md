# Google Cloud setup (step by step)

You do this once. Allow about 30 minutes. The pipeline loads very little data, so costs are tiny, but always set a budget alert (step 2).

## 1. Create a project
1. Go to console.cloud.google.com and sign in.
2. Click the project picker at the top, then **New project**. Name it, for example, `air-quality-pipeline`.
3. Note the **Project ID** (it is shown under the name and is usually not the same as the name). You will need it later.

## 2. Set a budget alert (do this first)
1. Open **Billing** in the left menu. Some features need a billing account linked to the project.
2. Go to **Budgets & alerts**, click **Create budget**, set a very small amount, and keep the email alerts on.
3. If you do not want to enable billing, you can skip Cloud Storage (step 5) and use only BigQuery. Google offers a BigQuery sandbox for trying it without a credit card. Check the current limits at cloud.google.com/bigquery/docs/sandbox, because they can change.

## 3. Install the Google Cloud CLI and log in
1. Install it from cloud.google.com/sdk/docs/install and open a new terminal.
2. Run:
```bash
gcloud init
gcloud auth application-default login
```
The second command opens your browser and saves credentials on your computer. The Python libraries find them automatically, so **you never put a key file in your project**.

## 4. Enable the BigQuery API
In the console, open **APIs & Services -> Library**, search for **BigQuery API** and click **Enable** (it is often already on).

## 5. Create a Cloud Storage bucket (optional)
Bucket names must be globally unique, for example `yourname-air-quality-lake-123`.
```bash
gcloud storage buckets create gs://YOUR_BUCKET_NAME --location=US
```
You can also do this in the console under **Cloud Storage -> Buckets -> Create**. Keep the location the same as `BQ_LOCATION` (default `US`).

## 6. Configure the project
```bash
copy .env.example .env      # Mac/Linux: cp .env.example .env
```
Open `.env` and fill in:
```
GCP_PROJECT_ID=your-project-id
BQ_DATASET=air_quality
BQ_TABLE=daily_air_quality
BQ_LOCATION=US
GCS_BUCKET=your-bucket-name     # leave empty to skip Cloud Storage
```

## 7. Run it
```bash
pip install -r requirements.txt
python run_pipeline.py --mode gcp
```
Expected: the five steps print, then query results appear. If you see a permissions error, check that you are logged in to the right account (`gcloud auth list`) and that the Project ID is correct.

## 8. Check the result in the console
1. Open **BigQuery**, then in the Explorer panel open your project -> `air_quality` -> `daily_air_quality`.
2. **Schema** tab: the 14 columns. **Details** tab: you should see *Partitioned by Day on reading_date* and *Clustered by city*. **Preview** tab: sample rows.
3. Click **Query** and run:
```sql
SELECT city, ROUND(AVG(pm25), 2) AS avg_pm25
FROM `YOUR_PROJECT_ID.air_quality.daily_air_quality`
GROUP BY city
ORDER BY avg_pm25 DESC;
```
4. Take screenshots of the table details and a query result for your README.

## 9. Clean up when you are done
```bash
bq rm -r -f -d YOUR_PROJECT_ID:air_quality
gcloud storage rm --recursive gs://YOUR_BUCKET_NAME
```
Or delete the whole project from **IAM & Admin -> Settings -> Shut down**.

## Common problems
| Error | Fix |
|---|---|
| `DefaultCredentialsError` | Run `gcloud auth application-default login` again |
| `403 ... Access Denied` | Wrong project ID, or your account lacks BigQuery permissions on it |
| `404 Not found: Dataset` or location errors | Keep `BQ_LOCATION` the same everywhere (bucket and dataset) |
| `Bucket does not exist` | Create the bucket (step 5) or leave `GCS_BUCKET` empty |
| Billing required | Link a billing account, or skip Cloud Storage and use the BigQuery sandbox |
