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
| **PySpark** | The Python way to use Apache Spark, an engine that processes tables (DataFrames) in parallel. |
| **Dataproc** | Google Cloud's managed service for running Spark (and Hadoop). It comes with Spark and Java installed, so you only write Python. |
| **Dataproc Serverless (batch)** | You submit a Spark job and Google runs it on workers it starts for you, then removes them. There is no cluster to create or delete, and you pay only while the job runs. Also called a *batch workload*. |
| **Runtime version** | The version of the Dataproc Serverless environment (it fixes the Spark version). We pin it in `.env` so runs are repeatable. |
| **Driver and executors** | The driver is the Spark program that plans the work. The executors are the workers that do it in parallel. |
| **`--py-files`** | The list of extra Python files or zips that Dataproc puts on the job's Python path. Our `pipeline.zip` travels this way. |
| **Lazy evaluation** | Spark only writes down a plan for `filter` and `withColumn`. It runs the plan when you call an action such as `count()` or `write`. |
| **Window function** | A calculation over a group of rows, for example numbering the rows of each (city, day) with `row_number()`. |

## 2. Read the code in this order
1. `scripts/generate_sample_data.py`: see how messy the raw data is. Open the CSV and spot the problems.
2. `src/pipeline/rules.py`: the schema, the lookup tables and the simple Python rules.
3. `src/pipeline/extract.py`: how the raw file is read as text.
4. `src/pipeline/transform.py`: each cleaning rule is numbered. Call `df.show()` before and after a rule to see what it changed.
5. `src/pipeline/quality.py`: the checks.
6. `src/pipeline/job.py` and `jobs/air_quality_job.py`: the Spark job that Dataproc runs. `jobs/air_quality_job.py` is only the start file; `job.py` joins extract, transform, the quality gate and the writing.
7. `src/pipeline/dataproc.py`: how the code is packed into a zip and how `gcloud dataproc batches submit` is called.
8. `src/pipeline/load_gcp.py`: the Cloud Storage and BigQuery calls.
9. `src/pipeline/pipeline.py` and `run_pipeline.py`: the manager on your computer that does upload -> Dataproc -> BigQuery -> download.
10. `tests/`: how the rules are proven, and how the order of the steps and the failure case are tested without any cloud.

## 3. Exercises (do at least three, commit each one separately)
1. **Read the job logs.** Open Dataproc -> Serverless -> Batches, click your batch, and find the driver output. Find the data-quality report in it, and the line where Spark started.
2. **Break it on purpose.** Add a row with a negative AQI that the cleaning does not remove, or lower a quality limit, and run again. Watch the job fail, the load get skipped, and `outputs/quality_report.json` still appear.
3. **Use real data.** Download a public city-level air-quality CSV with the same columns, put it in `data/raw/`, and rerun. What new problems appear?
4. **New SQL.** In the BigQuery console, try a 7-day rolling average: `AVG(pm25) OVER (PARTITION BY city ORDER BY reading_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)`.
5. **New quality check.** For example, fail if any city has fewer than 300 readings in a year. Add a test.
6. **Parquet.** Write Parquet with `df.write.parquet(...)` (Spark has this built in) instead of CSV, and load it into BigQuery. Compare file sizes.
7. **Incremental load.** Load one day of new data into a staging table and `MERGE` it into the main table.
8. **Schedule it.** Trigger the same Dataproc batch from Cloud Scheduler plus Workflows, or write an Airflow DAG for Cloud Composer (it has an operator for Dataproc Serverless batches).
9. **Dashboard.** Connect Looker Studio to the BigQuery table and chart monthly PM2.5 by city.
10. **Cost check.** Run a query with and without a `WHERE reading_date BETWEEN ...` filter and compare the bytes processed shown in the console. Then look at the Billing report for the Dataproc batch.

## 4. Interview questions you should be able to answer
1. **What does your pipeline do?** One command starts it. The raw CSV goes to Cloud Storage, a PySpark job on Dataproc Serverless cleans and validates it, the clean data is written back to Cloud Storage, and BigQuery loads it into a partitioned and clustered table.
2. **Why Cloud Storage and BigQuery both?** Storage is the cheap landing zone for raw and processed files (a data lake); BigQuery is the warehouse for fast SQL.
3. **Why partition and cluster?** Partitioning by day lets date-filtered queries scan fewer partitions (lower cost); clustering by city speeds up city filters.
4. **What does idempotent mean here?** `WRITE_TRUNCATE` replaces the table, so re-running never duplicates rows. The Spark job also overwrites its output folder.
5. **How do you handle bad data?** Documented cleaning rules that each count what they change, a quality gate that stops the load on failure, and a JSON report that is written even when the gate fails.
6. **Why an explicit schema?** Auto-detection can guess wrong, for example reading a column with blanks as a string.
7. **How does authentication work?** Two identities. My own login (`gcloud auth login` and Application Default Credentials) is used by my computer to upload, submit the job and load BigQuery. The Dataproc batch runs as a service account (the default Compute Engine one), which needs Dataproc Worker and access to the bucket. No key files in the repo. In production I would use a dedicated service account with minimal roles.
8. **How did you test it?** The plain-Python rules, the Dataproc command and zip, and the order of the pipeline steps and its failure handling are unit-tested without any cloud, using stand-ins for Google. The Spark cleaning code is checked on the real Dataproc run: `compare_with_expected.py` compares every output cell with the expected output. I also confirm the real load in the BigQuery console.
9. **How would you schedule it?** Cloud Scheduler calling Workflows to start the Dataproc batch, or an Airflow DAG on Cloud Composer. Right now I run it on demand with one command.
10. **How would you scale it?** The job already runs on Spark in Google Cloud and reads and writes `gs://` paths. For more data I would give the batch more resources, write Parquet instead of one CSV (drop `coalesce(1)`), and load incrementally with `MERGE`. Dataflow (Apache Beam) is the option for streaming data.
11. **How do you control BigQuery cost?** Filter on the partition column, select only needed columns, avoid `SELECT *`, and set budget alerts.
12. **What are the limits of the project?** Batch only, small synthetic data, simplified AQI formula, run on demand rather than scheduled, and the raw file is copied to the bucket from my computer instead of landing there from another system.
13. **Why Spark for such a small file?** Honestly, a simple single-machine script would be enough for this size. I used Spark so the same code can scale to data that does not fit on one machine, and to practise the Spark API: DataFrames, window functions and lazy evaluation.
14. **What is lazy evaluation?** Spark's transformations such as `filter` and `withColumn` only build a plan. Spark runs it when an action such as `count()` or `write` is called, and it can optimise the whole plan first. I call `cache()` on tables I use several times so the work is not repeated.
15. **How do you remove duplicates in Spark?** A window function: `row_number()` over a window partitioned by city and date and ordered by the original row position. Rows with number 2 or more are duplicates.
16. **Why `coalesce(1)` when writing the CSV?** Spark normally writes one part file per partition. I want one file in the bucket, and the table is small. (BigQuery can also load many part files with a `part-*.csv` wildcard, which is what the load step uses.) For big data I would keep the part files or write Parquet.
17. **Where does your Spark run, and how does it start?** Spark runs on Dataproc Serverless in Google Cloud. My script uploads the raw file, the start file and a zip of the code to Cloud Storage, then calls `gcloud dataproc batches submit pyspark` with the paths as arguments. Dataproc starts the driver and workers, runs the job, and removes them. Spark and Java are already in the Dataproc runtime, so nothing is installed on my computer except Python and the gcloud CLI.
18. **Why Dataproc Serverless and not a cluster?** For a short batch job there is nothing to create, size, or remember to delete, and I pay only while it runs. A cluster makes sense for many jobs that share one long-running setup, or when I need to control the machines and install special software.
19. **How does your code get onto Dataproc?** The start file `jobs/air_quality_job.py` and a zip of the `pipeline` package go to the bucket. The start file is the main file of the batch, and the zip is passed with `--py-files`, so `import pipeline` works on the workers.
20. **What happens when the job fails?** `gcloud` returns an error, the manager stops before the BigQuery load, tries to download the report from the bucket, and shows where to read the logs (Dataproc -> Serverless -> Batches, or `gcloud dataproc batches describe`). Nothing partial reaches BigQuery.
21. **Why not Composer?** I run it with one command so I can learn the job itself first. Composer (Airflow) is for scheduling and dependencies between many jobs, and it costs more to keep running than a single small job needs. It is my next step if the pipeline had to run daily.

## 5. Resume bullets (use only after you have run it on Google Cloud)
- Built an end-to-end batch data pipeline in Python and PySpark on Google Cloud: a Dataproc Serverless job cleans and validates raw city air-quality data with Spark DataFrames (date parsing, window-function deduplication, outlier handling, standardisation) and enforces a data-quality gate with a JSON report.
- Staged raw and processed files in Cloud Storage and loaded them into BigQuery (partitioned by date, clustered by city, explicit schema, idempotent loads), with the PySpark code packaged as a zip and submitted to Dataproc Serverless.
- Added unit tests (including a comparison of the Spark output with a reference output) and handled job-failure paths so bad data never reaches the warehouse.

Say "synthetic sample data" if an interviewer asks. If you use a real dataset, name it. Only claim what you have really run. You have run the Dataproc job from the console. The one-command run (`python run_pipeline.py`, which needs the gcloud CLI on your PC) is a separate path: describe it as built, and say it was tested only after you have run it yourself.

## 6. Put it on GitHub
In VS Code, open this folder, then Source Control -> Initialize Repository -> commit. Create an empty repo named `gcp-air-quality-pipeline` (no README) on github.com, then in the terminal:
```bash
git branch -M main
git remote add origin https://github.com/MohanNellore/gcp-air-quality-pipeline.git
git push -u origin main
```
Then add a repo description and topics (`gcp`, `dataproc`, `pyspark`, `bigquery`, `data-engineering`, `python`, `etl`, `google-cloud-storage`), and add your console screenshots (including the Dataproc batch page) to the README.

## 7. Free learning resources
- Google Cloud Skills Boost: free introductory labs for Dataproc, BigQuery and Cloud Storage.
- Google Cloud documentation: Dataproc Serverless for Apache Spark (batch workloads), BigQuery partitioned and clustered tables, loading CSV data.
- Google's entry-level data certification (check the Google Cloud site for the current name and requirements).
