"""
Perform ETL functions such as load tsv data to gen3 data portal (graphdb),
create diff summary, update from diff, or build external resource file from GDC REST API
"""
import sys
import os
import json
import logging
import urllib.parse
from dotenv import load_dotenv
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission

from load import adapt_and_load, is_valid_node_type
from update_data import get_differences, load_differences
from get_target_data import build_external_resource_file


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

# Local path to directory containing files to load
local_paths: list[str] = json.loads(os.environ.get('LOCAL_FILE_PATHS', '[]'))
local_path: str = os.environ.get('LOCAL_FILE_PATH')
file_type: str = os.environ.get('FILE_TYPE', 'tsv')

# base url to gen3 data portal
base_url: str = os.environ.get('BASE_URL', 'http://localhost')

# relative url to gen3 data portal json template endpoint
template_url: str = urllib.parse.urljoin(base_url, '/api/v0/submission/template/')

# node types to be loaded
node_types: list[str] = json.loads(os.environ.get('TYPES', '[]'))

# path to gen3 credentials file
credentials: str = os.environ.get('CREDENTIALS', '../credentials.json')


def load() -> None:
    """ Load gen3 data portal with credentials, url, and other information passed from config """
    auth: any = Gen3Auth(base_url, refresh_file=credentials)
    sub: any = Gen3Submission(base_url, auth)

    # suppress info log output from Gen3 API call to submit records
    logging.getLogger().setLevel(logging.WARNING)

    if local_paths and local_path:
        raise RuntimeError('Only one of LOCAL_FILE_PATH and LOCAL_FILE_PATHS may be specified')

    load_paths: list[str] = local_paths if local_paths else [local_path]
    load_path: str

    if not load_paths:
        raise RuntimeError('No load file path specified')

    if len(load_paths) > 1:
        logger.info('Loading from multiple directories:')
        for load_path in load_paths:
            logger.info(load_path)

    for load_path in load_paths:
        if not os.path.exists(load_path):
            raise RuntimeError(f'Directory "{load_path}" not found')

        if file_type not in ('tsv', 'json'):
            raise RuntimeError(f'Unsupported file type: "{file_type}"')

        # attempt to determine nodes to be loaded based on files in specified data directory
        node_files: list[str]
        local_files: list[str] = [f for f in os.listdir(load_path) if os.path.isfile(os.path.join(load_path, f))]
        if file_type == 'tsv':
            node_files = [f for f in local_files if f.startswith('gen3_') and f.endswith('.tsv')]
        elif file_type == 'json':
            node_files = [f for f in local_files if f.endswith('.json')]
            if 'program' not in node_files:
                node_files.insert(0, 'program')

        node_types_to_load: list[str] = []
        node_type: str
        node_file: str
        for node_file in node_files:
            if file_type == 'tsv':
                node_type = os.path.splitext(node_file)[0].split('gen3_')[-1]
            elif file_type == 'json':
                node_type = os.path.splitext(node_file)[0]
            node_types_to_load.append(node_type.strip().lower())

        # load nodes in alphabetical order and constrain to those in config ('TYPES' env var) if specified
        node_types_to_load = sorted([n for n in node_types_to_load if (not node_types or n in node_types)])
        invalid_node_types: list[str] = [n for n in node_types_to_load if not is_valid_node_type(n)]
        if invalid_node_types:
            raise RuntimeError(f'Invalid node types not in data dictionary found in {load_path}: {invalid_node_types}')

        # load program, project, subject, then timing nodes first if present
        priority_nodes: tuple[str, ...] = ('program', 'project', 'person', 'subject', 'timing')
        priority_node: str
        for priority_node in [n for n in reversed(priority_nodes) if n in node_types_to_load]:
            node_types_to_load.insert(0, node_types_to_load.pop(node_types_to_load.index(priority_node)))

        logger.info('Loading node types from %s: %s', load_path, node_types_to_load)
        for node_type in node_types_to_load:
            logger.info('Loading node type: %s', node_type)
            adapt_and_load(node_type, sub, template_url, load_path, file_type)


def differences() -> None:
    """ Determine differences between dictionaries of two gen3 data portal endpoints and save to local file """
    new_endpoint: str = 'http://localhost'
    old_endpoint: str = 'https://portal-dev.pedscommons.org'

    new_auth: Gen3Auth = Gen3Auth(new_endpoint, refresh_file='../credentials_compose.json')
    new_sub: Gen3Submission = Gen3Submission(new_endpoint, new_auth)

    old_auth: Gen3Auth = Gen3Auth(old_endpoint, refresh_file='../credentials_dev.json')
    old_sub: Gen3Submission = Gen3Submission(old_endpoint, old_auth)

    get_differences(old_sub, new_sub)

    print(
        'Please now update the DD on ' + old_endpoint + '. Once the secrets have been updated and the system roll ' +
        'is complete run `python etl.py update_data` to complete the process of updating the data in the graph DB'
    )

def load_diff_and_update() -> None:
    """ Apply changes to gen3 data portal records for differences determined by previous call to diff summary """
    old_endpoint: str = 'https://portal-dev.pedscommons.org'

    old_auth: Gen3Auth = Gen3Auth(old_endpoint, refresh_file='../credentials_dev.json')
    old_sub: Gen3Submission = Gen3Submission(old_endpoint, old_auth)

    load_differences(old_sub)
    print(
        'Now that the data have been updated, rememnber to execute ' +
        '`gen3 job run etl` in order to get the new data in the Exploration page.'
    )


if len(sys.argv) > 1:
    if sys.argv[1] == 'load':
        load()
    elif sys.argv[1] == 'create_summary':
        # README
        # This is meant to be used to generate a diff file.
        differences()
        print('Now please review the diff file and look for wrong values or wrong mappings.')
    elif sys.argv[1] == 'update_data':
        load_diff_and_update()
    elif sys.argv[1] == 'get_target_data':
        build_external_resource_file(local_path, template_url)
