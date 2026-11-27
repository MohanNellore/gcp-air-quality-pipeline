# Google Cloud setup and run (step by step, all in the console)

You do the setup once. Allow about 30 minutes. Everything happens in the Google Cloud console in your browser. You install **nothing** on your computer. The pipeline handles very little data, so costs are tiny, but always set a budget alert (step 2).

Replace `YOUR_PROJECT_ID` and `YOUR_BUCKET` with your own names. Wherever you see `PROJECT_NUMBER`, use your project number (shown on the console home page, in the **Project info** card).

## 1. Create a project
1. Go to console.cloud.google.com and sign in.
2. Click the project picker at the top, then **New project**. Name it, for example, `air-quality-pipeline`.
3. Note the **Project ID** (it is shown under the name and is usually not the same as the name).

## 2. Set a budget alert (do this first)
1. Open **Billing** in the left menu and link a billing account to the project. Dataproc and Cloud Storage need it.
2. Go to **Budgets & alerts**, click **Create budget**, set a very small amount, and keep the email alerts on.

## 3. Enable the APIs
Go to **APIs & Services -> Library**, then search for each of these and click **Enable**:
- **Cloud Dataproc API** (runs Spark)
- **Compute Engine API** (the network Dataproc uses)
- **BigQuery API**
- **Cloud Storage** (usually already on)

## 4. Create the Cloud Storage bucket
1. Go to **Cloud Storage -> Buckets -> Create**.
2. Name it (names must be unique worldwide), for example `yourname-air-quality-lake-123`.
3. Choose the location type **Multi-region** and **US**. Keep it the same as your BigQuery dataset location.
4. Click **Create**. In the bucket, create two folders with **Create folder**: `raw` and `code`.

## 5. Turn on Private Google Access
A Dataproc Serverless job runs on workers without public addresses, so the network must let them reach Google services (Cloud Storage, BigQuery).
1. Search for **VPC networks** and open it. Click the network **default**.
2. If there is no `default` network, wait a minute and refresh. If it is still missing, click **Create VPC network**, name it `default`, set **Subnet creation mode** to **Automatic**, tick the firewall rule that ends with **allow-internal**, and click **Create**.
3. Open the **Subnets** tab and click the subnet in the region you use (for example `us-central1`).
4. Click **Edit**, set **Private Google Access** to **On**, and click **Save**.

## 6. Give the job's service account its permissions
The job runs as the project's *default Compute Engine service account*, `PROJECT_NUMBER-compute@developer.gserviceaccount.com`.

**On the project:** go to **IAM & Admin -> IAM**, tick **Include Google-provided role grants**, click the pencil icon on that account, and add these three roles (**Add another role** for each):
- **Dataproc Worker**
- **BigQuery Data Editor**
- **BigQuery Job User**

**On the bucket:** open **Cloud Storage -> Buckets -> YOUR_BUCKET -> Permissions -> Grant access**. In **New principals** paste the same address (`PROJECT_NUMBER-compute@developer.gserviceaccount.com`), choose the role **Storage Object Admin**, and click **Save**.

Your own account needs no extra roles if you created the project (you are the Owner). Any other account needs **Dataproc Editor** and **Service Account User**.

## 7. Create the BigQuery dataset
1. Open **BigQuery**. In the Explorer panel click the three dots next to your project, then **Create dataset**.
2. Dataset ID: `air_quality`. Location: **US** (the same as the bucket). Click **Create dataset**.

The job creates the table by itself, so you do not create it.

## 8. Upload the two files
1. Get the files from GitHub: open the repo, click `air_quality_job.py` and `data/raw/air_quality_raw.csv`, and download each one (the download icon on the file page).
2. In your bucket open the `raw` folder, click **Upload files**, and choose `air_quality_raw.csv`.
3. Go back, open the `code` folder, click **Upload files**, and choose `air_quality_job.py`. When you change the code later, upload it again and replace the old one.

## 9. Submit the job
1. Search for **Dataproc** and open it. In the left menu click **Jobs (and batches)**, then open the **Serverless Spark** tab.
2. Click the blue **Submit job** button and fill in the form:

| Box | Value |
|---|---|
| Job ID | `air-quality-1` (lowercase letters, numbers and hyphens; use a new ID for every run) |
| Runtime profile | Leave the default (`Default service configuration`) |
| Job type | **PySpark** |
| Main Python file | `gs://YOUR_BUCKET/code/air_quality_job.py` |
| Arguments | Four arguments, each in its own box (press Enter after each): `--bucket`, then `YOUR_BUCKET`, then `--table`, then `YOUR_PROJECT_ID.air_quality.daily_air_quality` |

3. Leave everything else empty and click **Submit**.

The job shows **Pending**, then **Running**, then **Succeeded**. It takes a few minutes, and the first minute or two is Google preparing Spark. Click **Refresh** now and then.

To run it again, open the finished job and click **Clone**. Change only the **Job ID** and submit.

## 10. Check the result
1. **Logs.** Click the job ID, then **Job logs**. The data-quality report is printed there. For the sample file it shows 4,445 raw rows and 4,272 clean rows.
2. **Bucket.** You should see the folders `raw`, `code`, `processed` and `reports`. `processed/daily_air_quality_clean/` holds the clean CSV (a file starting with `part-`) and `reports/quality_report/` holds the JSON report.
3. **BigQuery.** Open `air_quality -> daily_air_quality`. The **Details** tab shows *Partitioned by Day on reading_date*, *Clustered by city* and the number of rows (4,272). The **Preview** tab shows sample rows.
4. **Query it.** Click **Query** and run:
```sql
SELECT city, ROUND(AVG(pm25), 2) AS avg_pm25
FROM `YOUR_PROJECT_ID.air_quality.daily_air_quality`
GROUP BY city
ORDER BY avg_pm25 DESC;
```
Some rows have empty PM2.5, PM10 or AQI. That is expected: a value that was missing in the raw file is left empty, and only AQI is calculated from PM2.5 when possible.

**Cost.** Dataproc Serverless bills for the resources used while the job runs. The job page shows the usage (my run of the sample file took 2 min 22 sec and used about 0.24 DCU-hours). Cloud Storage and BigQuery cost almost nothing at this size. Check **Billing -> Reports** after your first run.

## 11. Clean up when you are done
- Delete the dataset: in BigQuery click the three dots next to `air_quality`, then **Delete**.
- Delete the bucket: **Cloud Storage -> Buckets**, tick the bucket, **Delete**.
- There is no cluster to delete. Or delete the whole project from **IAM & Admin -> Settings -> Shut down**.

## Common problems
| Error | Fix |
|---|---|
| `API [dataproc.googleapis.com] not enabled` | Enable the Dataproc API (step 3) and wait a minute |
| The job fails at start with a message about the **subnet**, **network** or **Private Google Access** | Do step 5, then submit again with a new Job ID |
| The job starts, then fails reading or writing `gs://` (permission denied, 403) | The service account lacks the roles in step 6 (Dataproc Worker, and Storage Object Admin on the bucket) |
| `The clean CSV is saved in ..., but loading BigQuery failed` | The clean data is safe in the bucket. Check that the service account has **BigQuery Data Editor** and **BigQuery Job User** (step 6), that the dataset exists (step 7), and that the bucket and dataset are in the same location. Then clone the job and submit again |
| `Not found: Dataset` | Create the dataset (step 7), or check the `--table` argument |
| `--table must look like PROJECT.DATASET.TABLE` | The `--table` value needs two dots, for example `my-project.air_quality.daily_air_quality` |
| `Path does not exist` or `file not found` for the raw file | Upload the CSV to `YOUR_BUCKET/raw/` (step 8), or check the `--bucket` value |
| `Data quality checks failed` | The data broke a rule. Open `reports/quality_report/` in the bucket, download the `part-` file, and read `quality_checks` |
| The Job ID is rejected | Use 4 to 63 lowercase letters, numbers and hyphens, and a new ID for every run |
| Runtime version not supported | Dataproc retires old runtime versions. Pick a current one in the form's runtime profile |
| Billing required | Link a billing account to the project (step 2) |
