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


## Generating ES to DD Map

1. `cd path/to/gen3_etl/elasticsearch/`

2. `python -m venv env` (first time only)

3. `source env/bin/activate`

4. `pip install -r requirements-ES-DD.txt` (first time only)

5. generate .env file and populate 

   command line argument to generate file: `touch .env`

   Required Values:

      BASE_URL: Url to data portal  

      DICTIONARY_URL: Url to data dictionary in s3 bucket or <BASE_URL>/api/v0/submission/_dictionary/_all
         Note: the first optional allows you to use any data dictionary that still exists in s3
               while the second will always use the data dictionary currently in use
      
      OUTPUT_FILE: Path to where the map will be saved

      example:

      BASE_URL = 'https://portal-dev.pedscommons.org'
      DICTIONARY_URL='https://portal-dev.pedscommons.org/api/v0/submission/_dictionary/_all'
      OUTPUT_FILE='dd_map.json' 

6. `cd etl` and `python create_es_dd_mapping.py add-manual-fields`
   Note: add-manual-fileds is an optional argument if passed the values 
         in gen3_etl/elasticsearch/etl/manual_ES_to_DD_values.py will be added
