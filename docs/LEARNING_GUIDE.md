# Learning guide: understand this project before it goes on your resume

You must be able to explain every step in your own words. Do these in order.

## 1. Core ideas
| Term | Plain meaning |
|---|---|
| **ETL / ELT** | Extract data, Transform (clean) it, Load it. ELT loads first and transforms inside the warehouse. |
| **Data lake** | Cheap storage for files in any shape (Cloud Storage). |
| **Data warehouse** | Database built for analysis over big tables (BigQuery). |
| **Schema** | The column names and types of a table. The job sets them explicitly in its last cleaning step. |
| **Partitioning** | Splitting a table by a column (here, day) so queries read less data. |
| **Clustering** | Sorting data inside partitions (here, by city) so filters on it are faster. |
| **Idempotent** | Running the job twice gives the same result. `mode("overwrite")` does that. |
| **Data quality gate** | Checks that stop bad data before it is loaded. |
| **PySpark** | The Python way to use Apache Spark, an engine that processes tables (DataFrames) in parallel. |
| **Dataproc** | Google Cloud's managed service for running Spark (and Hadoop). It comes with Spark and Java installed, so you only write Python. |
| **Dataproc Serverless (batch)** | You submit a Spark job and Google runs it on workers it starts for you, then removes them. There is no cluster to create or delete, and you pay only while the job runs. |
| **Runtime version** | The version of the Dataproc Serverless environment (it fixes the Spark version). |
| **Driver and executors** | The driver is the Spark program that plans the work. The executors are the workers that do it in parallel. |
| **Spark BigQuery connector** | A library, already inside Dataproc, that lets Spark read from and write to BigQuery with `format("bigquery")`. |
| **Service account** | A robot identity. The Dataproc job runs as one, and it needs IAM roles to reach the bucket and BigQuery. |
| **IAM role** | A named set of permissions, for example Storage Object Admin or BigQuery Data Editor. |
| **Lazy evaluation** | Spark only writes down a plan for `filter` and `withColumn`. It runs the plan when you call an action such as `count()` or `write`. |
| **Window function** | A calculation over a group of rows, for example numbering the rows of each (city, day) with `row_number()`. |

## 2. Read the code in this order
Everything is in `air_quality_job.py`. Read its numbered parts from top to bottom:
1. Open `data/raw/air_quality_raw.csv`: see how messy the raw data is. Spot the missing values, negatives, duplicates, odd city names and bad dates.
2. **Part 0, tables and settings:** the final columns and types, the city-name table and the AQI tables.
3. **Part 1, extract:** how the raw file is read as text.
4. **Part 2, transform:** each cleaning rule is numbered. Read `clean()` at the end first, then each rule above it. Add `df.show()` before and after a rule to see what it changed (in a notebook or a test copy).
5. **Part 3, quality:** the six checks.
6. **Part 4, load:** writing the CSV, the report and the BigQuery table.
7. **`run_job()` and `main()`:** how the four parts connect, and how the Arguments boxes become Python values.

## 3. Exercises (do at least three)
1. **Read the job logs.** Open your job in Dataproc, click **Job logs**, and find the data-quality report and the lines `[1/4]` to `[4/4]`.
2. **Break it on purpose.** Submit a job with a wrong table name such as `bad_table`, and read the error. Then upload a CSV with a missing column and read that error.
3. **Use real data.** Upload a public city-level air-quality CSV with the same columns and run again. What new problems appear?
4. **New SQL.** In BigQuery try a 7-day rolling average: `AVG(pm25) OVER (PARTITION BY city ORDER BY reading_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`.
5. **New quality check.** For example, fail if any city has fewer than 300 readings in a year. Add it to part 3 and watch the job fail on a small file.
6. **Parquet.** Write Parquet with `df.write.parquet(...)` instead of CSV and compare file sizes.
7. **Incremental load.** Load one day of new data into a staging table and `MERGE` it into the main table.
8. **Schedule it.** Trigger the same Dataproc batch from Cloud Scheduler plus Workflows, or write an Airflow DAG for Cloud Composer.
9. **Dashboard.** Connect Looker Studio to the BigQuery table and chart monthly PM2.5 by city.
10. **Cost check.** Run a query with and without a `WHERE reading_date BETWEEN ...` filter and compare the bytes processed. Then look at the Billing report for the Dataproc job.

## 4. Interview questions you should be able to answer
1. **What does your pipeline do?** One PySpark job on Dataproc Serverless reads a raw CSV from Cloud Storage, cleans and validates it, writes the clean CSV and a quality report back to Cloud Storage, and loads a partitioned and clustered BigQuery table.
2. **Why Cloud Storage and BigQuery both?** Storage is the cheap landing zone for raw and processed files (a data lake); BigQuery is the warehouse for fast SQL.
3. **Why partition and cluster?** Partitioning by day lets date-filtered queries scan fewer partitions (lower cost); clustering by city speeds up city filters.
4. **What does idempotent mean here?** The job overwrites its output folders and the BigQuery table, so re-running never duplicates rows.
5. **How do you handle bad data?** Documented cleaning rules that each count what they change, a quality gate that stops the job before anything is written to `processed/` or BigQuery, and a JSON report that is written even when the gate fails.
6. **Why explicit column types?** Auto-detection can guess wrong, for example reading a column with blanks as a string. The last cleaning step casts every column to its final type, so BigQuery gets the schema I chose.
7. **How does permission work?** The job runs as the project's default Compute Engine service account. I gave it Dataproc Worker, Storage Object Admin on the bucket, and BigQuery Data Editor and Job User. No key files are used anywhere. In production I would create a dedicated service account with minimal roles.
8. **How did you test it?** I ran the job on Dataproc and checked the results: the quality report in the logs, the files in the bucket, and the BigQuery table (row count, partitioning, clustering, and the counts of empty values). The job also has a built-in data-quality gate. I did not write unit tests, because the cleaning code needs Spark with Java; that is a next step.
9. **How would you schedule it?** Cloud Scheduler calling Workflows to start the Dataproc batch, or an Airflow DAG on Cloud Composer. Right now I start it by hand in the console.
10. **How would you scale it?** The job already runs on Spark in Google Cloud and reads and writes `gs://` paths. For more data I would give the batch more resources, write Parquet instead of one CSV (drop `coalesce(1)`), and load incrementally with `MERGE`. Dataflow (Apache Beam) is the option for streaming data.
11. **How do you control BigQuery cost?** Filter on the partition column, select only needed columns, avoid `SELECT *`, and set budget alerts.
12. **What are the limits of the project?** Batch only, small synthetic data, simplified AQI formula, started by hand rather than scheduled, no unit tests, and the raw file is uploaded by hand instead of landing in the bucket from another system.
13. **Why Spark for such a small file?** Honestly, a simple single-machine script would be enough for this size. I used Spark so the same code can scale to data that does not fit on one machine, and to practise the Spark API: DataFrames, window functions and lazy evaluation.
14. **What is lazy evaluation?** Spark's transformations such as `filter` and `withColumn` only build a plan. Spark runs it when an action such as `count()` or `write` is called, and it can optimise the whole plan first. I call `cache()` on tables I use several times so the work is not repeated.
15. **How do you remove duplicates in Spark?** A window function: `row_number()` over a window partitioned by city and date and ordered by the original row position. Rows with number 2 or more are duplicates.
16. **Why `coalesce(1)` when writing the CSV?** Spark normally writes one part file per partition. I want one file in the bucket, and the table is small. For big data I would keep the part files or write Parquet.
17. **Where does your Spark run?** On Dataproc Serverless in Google Cloud. I upload the raw CSV and one Python file to a bucket, submit the file as a PySpark batch in the console, and Dataproc starts the driver and workers, runs the job, and removes them. Spark and Java are already in the Dataproc runtime, so nothing is installed on my computer.
18. **Why Dataproc Serverless and not a cluster?** For a short batch job there is nothing to create, size, or remember to delete, and I pay only while it runs. A cluster makes sense for many jobs that share one long-running setup, or when I need to control the machines and install special software.
19. **How does the job write to BigQuery?** With the Spark BigQuery connector that is built into Dataproc. I use the indirect method: the connector writes temporary files into my bucket, BigQuery loads them in one operation, and the temporary files are deleted. I set the partition column (`reading_date`, by day) and the clustering column (`city`) as options, and use overwrite mode.
20. **What happens when the job fails?** The job stops with an error in the Dataproc logs, and its status is Failed. If a quality check fails, the report is still written to the bucket and nothing is written to `processed/` or BigQuery. If only the BigQuery load fails, the clean CSV is already saved and I can fix the permission or the name and run the job again.
21. **Why not Composer?** I start it by hand so I can learn the job itself first. Composer (Airflow) is for scheduling and dependencies between many jobs, and it costs more to keep running than a single small job needs. It is my next step if the pipeline had to run daily.

## 5. Resume bullets (use only after you have run it on Google Cloud)
- Built an end-to-end batch data pipeline in Python and PySpark on Google Cloud: a single Dataproc Serverless job cleans and validates raw city air-quality data with Spark DataFrames (date parsing, window-function deduplication, outlier handling, standardisation) and enforces a data-quality gate with a JSON report.
- Staged raw and processed data in Cloud Storage and loaded it into BigQuery from Spark (partitioned by date, clustered by city, idempotent overwrite), with IAM roles set up for the job's service account.

Say "synthetic sample data" if an interviewer asks, and say that you built the project with AI assistance and can explain every part. If you use a real dataset, name it. Only claim what you have really run.

## 6. GitHub
The repo is already on GitHub. Add a repo description and topics (`gcp`, `dataproc`, `pyspark`, `bigquery`, `data-engineering`, `python`, `etl`, `google-cloud-storage`), and keep your console screenshots (the job, the bucket and the BigQuery table) in the README.

## 7. Free learning resources
- Google Cloud Skills Boost: free introductory labs for Dataproc, BigQuery and Cloud Storage.
- Google Cloud documentation: Serverless for Apache Spark (batch workloads), the Spark BigQuery connector, BigQuery partitioned and clustered tables.
- Google's entry-level data certification (check the Google Cloud site for the current name and requirements).
