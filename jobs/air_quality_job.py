"""The PySpark job that Dataproc runs.

Dataproc runs this file. It also runs on your own computer if you have Spark (and Java) installed:

  python jobs/air_quality_job.py --input data/raw/air_quality_raw.csv \
      --processed outputs/job/processed --report outputs/job/report

Arguments:
  --input      the raw CSV file (a gs:// path on Dataproc)
  --processed  the folder where the clean CSV is written
  --report     the folder where the JSON quality report is written
"""
import argparse
import os
import sys

# On your own computer, find our code in the src folder.
# On Dataproc our code arrives in a zip file (pipeline.zip), so this line has no effect there.
project_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(project_folder, "src"))

# These imports must stay below the line above, otherwise Python cannot find "pipeline"
from pipeline.job import run_job
from pipeline.spark_utils import get_spark


def main():
    # Read the options typed after the command
    parser = argparse.ArgumentParser(description="Air-quality PySpark job")
    parser.add_argument("--input", required=True)
    parser.add_argument("--processed", required=True)
    parser.add_argument("--report", required=True)
    args = parser.parse_args()

    spark = get_spark()
    try:
        run_job(spark, args.input, args.processed, args.report)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
