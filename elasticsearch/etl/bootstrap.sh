#!/bin/bash
set -e
python3 -m venv /home/hadoop/etl_venv
source /home/hadoop/etl_venv/bin/activate
# pyspark must be installed in the venv so the step driver and YARN executors
# can both import it. Use the same major.minor as the EMR runtime (3.5.x on EMR 7.x).
pip install gen3==4.5.0 python-dotenv "urllib3<2" requests "elasticsearch==7.10.0" "numpy<2" requests-aws4auth boto3 pyspark==3.5.0