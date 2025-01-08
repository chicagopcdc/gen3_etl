# Contents Description

## Set up the environment
1. Create a a gen3_scripts/gen3_load/.env file with the following values:
    - `DICTIONARY_URL` The dictionary URL
    - `PROGRAM_NAME` The name for the program
    - `PROJECT_CODE` The code for the project
    - `SAMPLE` Max number of smaple to create for each node 
    - `BASE_URL` the environemnt we are loading to, default is localhost
    - `TYPES` is defaulted to the array of nodes in quick_load 
    - `LOCAL_FILE_PATH` where the file are, default is "../fake_data/quick_load/"
    - `FILE_TYPE` default is "tsv"
2. Activate Python 3 virtual environment:
   - `python -m venv env` (first time only)
   - `source env/bin/activate`
3. Install the required dependencies:
   - `pip install -r requirements.txt` (first time only)
4. Create the API credentials file `./credentials.json` and save it in the same directory as this file:
   - Creating API keys is available on the Portal's "Profile" page (`/identity`)
   - `./credentials.json` should contain the following data:

    ```json
    {
      "api_key": "",
      "key_id": ""
    }
    ```

## Create fake data (if needed)
1. Run `bash ./generate.sh`
    - This script assumes the user is already logged in in Github.

## Operations
1. Load data
    - `cd operations` and `python etl.py load`
    - `bash ./guppy_setup.sh` BROKEN use `https://github.com/chicagopcdc/gen3_scripts/blob/pcdc_dev/es_etl_patch/README.md` instead






    


<!-- if running towards localhost set this env variable
REQUESTS_CA_BUNDLE={path/to/compose/} + /Secrets/TLS/ca.pem -->
