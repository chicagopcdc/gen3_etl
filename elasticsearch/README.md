# Contents Description

## Set up the environment

1. Activate Python 3 virtual environment:
   - `python -m venv env` (first time only)
   - `source env/bin/activate`
2. Install the required dependencies:
   - `pip install -r requirements.txt` (first time only)
3. replace the string `GITHUB_TOKEN` in etl/build_json.py with your Github token

## Operations

1. ETL data in ES from file (default to fake data in configuration-files repo)
    - `cd etl` and `python create_index.py`
    - `docker restart guppy-service`

2. ETL data from graphDB to ES
    - 
