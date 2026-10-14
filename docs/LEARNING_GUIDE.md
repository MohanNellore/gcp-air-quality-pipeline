# Learning guide: understand this project before it goes on your resume

You must be able to explain every step in your own words. Do these in order.

## 1. Core ideas
| Term | Plain meaning |
|---|---|
| **ETL / ELT** | Extract data, Transform (clean) it, Load it. ELT loads first and transforms inside the warehouse. |
| **Data lake** | Cheap storage for files in any shape (Cloud Storage). |
| **Data warehouse** | Database built for analysis over big tables (BigQuery). |
| **Schema** | The column names and types of a table. We set it explicitly. |
| **Partitioning** | Splitting a table by a column (here, day) so queries read less data. |
| **Clustering** | Sorting data inside partitions (here, by city) so filters on it are faster. |
| **Idempotent** | Running the job twice gives the same result. `WRITE_TRUNCATE` does that. |
| **Data quality gate** | Checks that stop bad data before it is loaded. |
| **ADC** | Application Default Credentials: your login is used by the Python libraries, no key file. |

## 2. Read the code in this order
1. `scripts/generate_sample_data.py`: see how messy the raw data is. Open the CSV and spot the problems.
2. `src/pipeline/transform.py`: each cleaning rule is numbered. Print the dataframe before and after.
3. `src/pipeline/quality.py`: the checks.
4. `sql/*.sql`: read each query and predict its output before you run it.
5. `src/pipeline/load_gcp.py`: the Cloud Storage and BigQuery calls.
6. `src/pipeline/pipeline.py` and `run_pipeline.py`: how it all connects.
7. `tests/`: how each rule is proven.

## 3. Exercises (do at least three, commit each one separately)
1. **Use real data.** Download a public city-level air-quality CSV with the same columns, put it in `data/raw/`, and rerun. What new problems appear?
2. **New SQL.** Add a 7-day rolling average: `AVG(pm25) OVER (PARTITION BY city ORDER BY reading_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`.
3. **New quality check.** For example, fail if any city has fewer than 300 readings in a year. Add a test.
4. **Parquet.** Install `pyarrow`, write Parquet instead of CSV, and load it into BigQuery. Compare file sizes.
5. **Incremental load.** Load one day of new data into a staging table and `MERGE` it into the main table.
6. **Schedule it.** Package it for Cloud Run and trigger it with Cloud Scheduler, or write an Airflow DAG for Cloud Composer.
7. **Dashboard.** Connect Looker Studio to the BigQuery table and chart monthly PM2.5 by city.
8. **Cost check.** Run a query with and without a `WHERE reading_date BETWEEN ...` filter and compare the bytes processed shown in the console.

## 4. Interview questions you should be able to answer
1. **What does your pipeline do?** Reads raw CSV, cleans and validates it with Pandas, stages it in Cloud Storage, loads it into a partitioned and clustered BigQuery table, and runs SQL analytics.
2. **Why Cloud Storage and BigQuery both?** Storage is the cheap landing zone for raw and processed files (a data lake); BigQuery is the warehouse for fast SQL.
3. **Why partition and cluster?** Partitioning by day lets date-filtered queries scan fewer partitions (lower cost); clustering by city speeds up city filters.
4. **What does idempotent mean here?** `WRITE_TRUNCATE` replaces the table, so re-running never duplicates rows.
5. **How do you handle bad data?** Documented cleaning rules that each count what they change, a quality gate that stops the load on failure, and a JSON report.
6. **Why an explicit schema?** Auto-detection can guess wrong, for example reading a column with blanks as a string.
7. **How does authentication work?** Application Default Credentials from `gcloud auth application-default login`. No key files in the repo. In production you would use a service account with minimal roles.
8. **How did you test without the cloud?** A SQLite stand-in runs the same SQL and the unit tests. Limit: dialects differ slightly, so I confirm queries in BigQuery.
9. **How would you schedule it?** Cloud Scheduler triggering a Cloud Run job, or an Airflow DAG on Cloud Composer.
10. **How would you scale it?** Parquet instead of CSV, incremental loads with `MERGE`, and Dataflow (Apache Beam) for very large or streaming data.
11. **How do you control BigQuery cost?** Filter on the partition column, select only needed columns, avoid `SELECT *`, and set budget alerts.
12. **What are the limits of the project?** Batch only, small synthetic data, simplified AQI formula, no orchestration yet.

## 5. Resume bullets (use only after you have run it on Google Cloud)
- Built an end-to-end batch data pipeline in Python: cleaned and validated raw city air-quality data with Pandas (date parsing, deduplication, outlier handling, standardisation) and added a data-quality gate and report.
- Loaded the data into Google BigQuery (partitioned by date, clustered by city, explicit schema, idempotent loads), staging files in Cloud Storage.
- Wrote analytical SQL (CTEs, window functions, ranking) for city and seasonal pollution trends; added unit tests and a local SQLite mode for cloud-free testing.

Say "synthetic sample data" if an interviewer asks. If you use a real dataset, name it. If you skip Cloud Storage, remove it from the bullet. Only claim what you have really run.

## 6. Put it on GitHub
In VS Code, open this folder, then Source Control -> Initialize Repository -> commit. Create an empty repo named `gcp-air-quality-pipeline` (no README) on github.com, then in the terminal:
```bash
git branch -M main
git remote add origin https://github.com/MohanNellore/gcp-air-quality-pipeline.git
git push -u origin main
```
Then add a repo description and topics (`gcp`, `bigquery`, `data-engineering`, `python`, `etl`, `sql`, `google-cloud-storage`), and add your console screenshots to the README.

## 7. Free learning resources
- Google Cloud Skills Boost: free introductory labs for BigQuery and Cloud Storage.
- Google Cloud documentation: BigQuery partitioned and clustered tables, loading CSV data.
- Google's entry-level data certification (check the Google Cloud site for the current name and requirements).
