"""
Perform ETL functions for gen3 data portal such as extract (retrieve from graphdb), transform (output
extracted graphdb datga to json), load (Elasticsearch index with json data), or switch aliases
"""
import sys
import json
import logging
import os
from dotenv import load_dotenv
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from transform import generate_subject_json
from transform import generate_es_index_mapping
from load import load_es_data
from load import load_es_array_config
from load import switch_alias


# set up logging
logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False
if len(logger.handlers) > 0:
    logger.handlers.clear()
logger.addHandler(logging.StreamHandler(sys.stdout))
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
for handler in logger.handlers:
    handler.setFormatter(formatter)

# Load env variables
load_dotenv('../.env')

# base url to gen3 data portal
base_url: str = os.environ.get('BASE_URL', 'http://localhost')

# list of projects to be handled
projects: list[str] = json.loads(os.environ.get('PROJECT_LIST', '["pcdc-20220808"]'))

# list of node types to be handled
node_types: list[str] = json.loads(os.environ.get('TYPES', '[]'))

# path to gen3 credentials file
credentials: str = os.environ.get('CREDENTIALS', '../credentials.json')

# path to local file where json data will be imported/exported
local_es_file_path: str = os.environ.get('LOCAL_ES_FILE_PATH', '../files/es_data.json')

# Elasticsearch port
es_port: int = int(os.environ.get('ES_PORT', 9200))

# Elasticsearch index name
index_name: str = os.environ.get('INDEX_NAME', 'pcdc_20220808')

# Elasticsearch parameters for bulk/batch api
es_bulk_batch_size: int = int(os.environ.get('ES_BULK_BATCH_SIZE', 10))
es_bulk_max_tries: int = int(os.environ.get('ES_BULK_MAX_TRIES', 3))
es_bulk_retry_delay: int = int(os.environ.get('ES_BULK_RETRY_DELAY', 60))
es_timeout: int = int(os.environ.get('ES_TIMEOUT', 60))

def extract() -> dict[str, any]:
    """ Extract gen3 data (graphdb) to json data dictionary """
    auth: Gen3Auth = Gen3Auth(base_url, refresh_file=credentials)
    sub: Gen3Submission = Gen3Submission(base_url, auth)

    node_data: dict[str, any] = {}
    project: str
    for project in projects:
        project_tokens: list[str] = project.split('-')
        if len(project_tokens) != 2:
            raise ValueError(f"Invalid project id: '{project}'")

        program_name: str = project_tokens[0]
        project_code: str = project_tokens[1]
        node_data[project] = {}
        node_type: str
        for node_type in node_types:
            logger.info('Extracting %s ...', node_type)
            node_data[project][node_type] = sub.export_node(program_name, project_code, node_type, 'json')['data']
    logger.info('Extract successful')
    return node_data


def transform(data: dict[str, any]) -> list[dict[str, any]]:
    """ Transform specified data extracted from gen3 data portal (graphdb) to json """
    logger.info('Generating Elasticsearch index mapping file nested_mapping.json')
    es_mapping: dict[str, any] = generate_es_index_mapping()
    parent_dir: str = os.path.dirname(local_es_file_path) if local_es_file_path else './'
    es_index_mapping_file: str = os.path.join(parent_dir, 'nested_mapping.json')
    with open(es_index_mapping_file, 'w', encoding='utf-8') as mapping_file:
        json.dump(es_mapping, mapping_file)
    return generate_subject_json(data, node_types)


def load(data: dict[str, any]) -> None:
    """ Load gen3 data portal Elasticsearch data and array config indexes  with (extracted, transformed) json data """
    load_data(data)
    load_array_config()


def load_data(data: dict[str, any]) -> None:
    """ Load gen3 data portal Elasticsearch data index with (extracted, transformed) json data """
    load_es_data(data, es_port, index_name, es_bulk_batch_size, es_bulk_max_tries, es_bulk_retry_delay)


def load_array_config() -> None:
    """ Load gen3 data portal Elasticsearch array config index with array fields """
    load_es_array_config(es_port, index_name)


if len(sys.argv) > 1:
    if sys.argv[1] == 'e':
        logger.info('Extracting data')
        extracted_data: dict[str, any] = extract()
        if len(sys.argv) == 3:
            save_file_path: str = sys.argv[2]
            logger.info('Saving extracted data to %s', save_file_path)
            with open(save_file_path, 'w', encoding='utf-8') as out_file:
                json.dump(extracted_data, out_file)
        else:
            logger.fatal('Usage: python etl.py e [extract save file path]')
    elif sys.argv[1] == 't':
        if len(sys.argv) == 3:
            saved_extract_file_path: str = sys.argv[2]
            if not os.path.exists(saved_extract_file_path):
                logger.fatal("Saved extract file '%s' not found", saved_extract_file_path)
            else:
                logger.info("Loading saved extract file '%s'", saved_extract_file_path)
                with open(saved_extract_file_path, 'r', encoding='utf-8') as in_file:
                    extracted_data: dict[str, any] = json.load(in_file)
                    es_data: list[dict[str, any]] = transform(extracted_data)
                    if local_es_file_path:
                        logger.info('Saving transformed ES data to %s', local_es_file_path)
                        with open(local_es_file_path, 'w', encoding='utf-8') as out_file:
                            json.dump(es_data, out_file)
                    else:
                        logger.warning('Data transformed but not saved, specify LOCAL_ES_FILE_PATH to save')
        else:
            logger.fatal('Usage: python etl.py t [saved extract file path]')
    elif sys.argv[1] == 'et':
        logger.info('Extracting and transforming data')
        es_data: list[dict[str, any]] = transform(extract())
        if local_es_file_path:
            logger.info('Saving extracted and transformed ES data to %s', local_es_file_path)
            with open(local_es_file_path, 'w', encoding='utf-8') as out_file:
                json.dump(es_data, out_file)
        else:
            logger.warning('Data extracted and transformed but not saved, specify LOCAL_ES_FILE_PATH to save')
    elif sys.argv[1] == 'etl':
        logger.info('Extracting and transforming data')
        es_data: list[dict[str, any]] = transform(extract())
        logger.info('Loading Elasticsearch index %s', index_name)
        load(es_data)
    elif sys.argv[1] == 'l':
        # load data and array config
        if local_es_file_path:
            logger.info('Loading Elasticsearch index %s with data contained in %s', index_name, local_es_file_path )
            with open(local_es_file_path, encoding='utf-8') as f:
                es_data: list = json.load(f)
                load(es_data)
        else:
            logger.warning("Nothing to do, specify 'LOCAL_ES_FILE_PATH' to load")
    elif sys.argv[1] == 'lac':
        # load array config only
        load_array_config()
    elif sys.argv[1] == 'ld':
        # load data only
        if local_es_file_path:
            logger.info('Loading Elasticsearch index %s with data contained in %s', index_name, local_es_file_path )
            with open(local_es_file_path, encoding='utf-8') as f:
                es_data: list[dict[str, any]] = json.load(f)
                load_data(es_data)
        else:
            logger.warning("Nothing to do, specify 'LOCAL_ES_FILE_PATH' to load")
    elif sys.argv[1] == 'a':
        # re-assign pcdc and pcdc-array-config aliases from current/old to new index
        if len(sys.argv) == 4 and sys.argv[2] and sys.argv[3]:
            logger.info(
                "Switching Elasticsearch aliases for 'pcdc' and 'pcdc-array-config' from '%s' to '%s'",
                sys.argv[2],
                sys.argv[3]
            )
            switch_alias(es_port, sys.argv[2], sys.argv[3])
        else:
            logger.fatal('Usage: python etl.py a [old_index] [new_index]')
