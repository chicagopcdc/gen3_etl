""" Load gen3 data portal graphdb """
import csv
import json
import logging
import os
import sys
import time

import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.SecurityWarning)


logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False
if len(logger.handlers) > 0:
    logger.handlers.clear()
logger.addHandler(logging.StreamHandler(sys.stdout))
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
for handler in logger.handlers:
    handler.setFormatter(formatter)

number_fields: list[str] = []
array_fields: list[str] = []
data_dict: dict[str, any] = {}
total_failed_submit_attempts: int = 0


def load_data_dict(data_dict_url: str = None) -> None:
    """
    Load data dictionary from specified URL or config if URL not specified
    In terms of properties of interest, the dictionary is of the form:
    { "entity_type.yaml": { "properties": { "property_name": { "enum": { "value1" ... } ... } ... } ... } ... }

    for example:
    { "lab.yaml": { "properties": { ... "lab_spec_type": { "enum": { "Blood", ... } ... } ... } ... } ... }
    """
    data_dict_url: str = data_dict_url if data_dict_url else os.environ.get('DICTIONARY_URL', None)
    if not data_dict_url:
        raise RuntimeError('Unable to retrieve data dictionary')

    logger.info("Loading data dictionary from '%s'", data_dict_url)
    try:
        # bypass requests_ca_bundle to retrieve data dict from external (e.g. S3) source during local env ETL
        requests_ca_bundle: str = os.environ.get('REQUESTS_CA_BUNDLE', '')
        if requests_ca_bundle and 'localhost' not in data_dict_url.lower():
            os.environ['REQUESTS_CA_BUNDLE'] = ''
        response: requests.Response = requests.get(data_dict_url, timeout=180)
        if requests_ca_bundle and 'localhost' not in data_dict_url.lower():
            os.environ['REQUESTS_CA_BUNDLE'] = requests_ca_bundle
        if not response.ok:
            response.raise_for_status()
        number_fields.clear()
        array_fields.clear()
        dd: dict[str, any] = response.json()
        data_dict.clear()
        data_dict.update(dd)
    except requests.exceptions.HTTPError as http_error:
        logger.error('Error retrieving data dictionary JSON:')
        logger.exception(http_error)
        raise
    except requests.exceptions.JSONDecodeError as json_decode_error:
        logger.error('Error decoding data dictionary JSON:')
        logger.exception(json_decode_error)
        raise


def load_field_type_lists() -> None:
    """
    Load the number and array field properties from data dictionary
    """
    if not data_dict:
        raise RuntimeError('Data dictionary not populated')

    node_type_name: str
    node_type_props: dict[str, any]
    for node_type_name, node_type_props in data_dict.items():
        # timing.yaml, lab.yaml, etc
        if node_type_name.startswith('_') or 'properties' not in node_type_props:
            continue
        field: str
        for field in node_type_props['properties']:
            if 'type' not in node_type_props['properties'][field]:
                continue
            field_type: any = node_type_props['properties'][field]['type']
            if isinstance(field_type, str):
                if field_type == 'number':
                    number_fields.append(field)
                elif field_type == 'array':
                    array_fields.append(field)
            elif isinstance(field_type, list):
                if 'number' in field_type:
                    number_fields.append(field)
                elif 'array' in field_type:
                    array_fields.append(field)


def to_num(val: any) -> any:
    """ Attempt to convert specified input value to number by first trying int then float """
    try:
        return int(val)
    except ValueError:
        return float(val)


def to_array(val: any, delimiter: str = '|') -> list:
    """ Convert specified input value to array by splitting on delimiter (default '|') and trim (strip) elements """
    try:
        values: list = val.split(delimiter) if not isinstance(val, list) else val
        return [v.strip() for v in values]
    except ValueError as verr:
        logger.error('Error transforming in array value: %s', val)
        logger.error(verr)
        return None


def is_valid_node_type(node_type: str) -> bool:
    """ Verify that specified node type is valid according to data dictionary """
    if not data_dict:
        load_data_dict(os.environ.get('DICTIONARY_URL', None))

    if not data_dict:
        raise RuntimeError('Unable to load data dictionary')

    return f'{node_type}.yaml' in data_dict


def adapt_and_load(node_type: str, gen3_sub: any, template_url: str, local_path: str, file_type: str) -> None:
    """
    Load specified node type to gen3 portal (graphdb) for specified json object template url, input file path and type
    """
    # pylint: disable-next=invalid-name, global-statement
    global total_failed_submit_attempts
    if not local_path or not os.path.exists(local_path):
        logger.error('File not found: %s', local_path)
        return

    # load data dictionary
    if not data_dict:
        load_data_dict(os.environ.get('DICTIONARY_URL', None))

    if not data_dict:
        raise RuntimeError('Unable to load data dictionary')

    # validate node type
    if not is_valid_node_type(node_type):
        logger.warning("Node type '%s' not found in data dictionary, skipping load", node_type)
        return

    # populate list of number and array properties identified in data dictionary
    if not number_fields and not array_fields:
        load_field_type_lists()

    if not number_fields and not array_fields:
        raise RuntimeError('Unable to populate number and array field type lists from data dictionary')

    new_entities: list = []
    missing_columns: dict = {}
    file_path: str
    if file_type == 'tsv':
        file_path = os.path.join(local_path, f'gen3_{node_type}.tsv')
        if not os.path.exists(file_path):
            logger.warning("File '%s' not found, node type '%s' not loaded", file_path, node_type)
            return

        with open(file_path, encoding='utf-8') as tsvfile:
            reader: any = csv.DictReader(tsvfile, dialect='excel-tab')

            # load json templates from portal template url
            json_str: str = requests.get(template_url + node_type + '?format=json', timeout=180)
            template_obj: dict[str, any] = json_str.json()

            # match each row in the tsv file with the correct item in the json template and fill in the attributes
            tsv_row: dict[str, any]
            # pylint: disable-next=too-many-nested-blocks
            for tsv_row in reader:
                new_entity: dict[str, any] = {}
                template_field: str
                for template_field in template_obj:
                    # remove * that indicates required field when present in header to avoid missing match
                    # or getting BAD date request since * is not supposed to be in submitted file
                    entity_field: str = template_field
                    if template_field[0] == '*':
                        entity_field = template_field[1:]

                    # if the item is an object (e.g. timings or subjects) populate the required field value with
                    # the field name having form 'type_collection.field_name' such as 'subjects.submitter_id'
                    if isinstance(template_obj[template_field], dict):
                        possible_tsv_fields: list = [k for k in tsv_row if k is not None and f'{template_field}.' in k]
                        if len(possible_tsv_fields) == 0:
                            # if template_field not in missing_columns:
                            #     missing_columns[template_field] = []
                            missing_columns[template_field] = tsv_row['*submitter_id']
                            continue
                        if not tsv_row[possible_tsv_fields[0]]:
                            continue

                        new_entity[entity_field] = template_obj[template_field].copy()
                        keys: list = possible_tsv_fields[0].split('.')
                        if len(keys) > 2:
                            logger.warning(
                                "linked property name not in expected form 'type_collection.key_name': %s",
                                possible_tsv_fields
                            )
                        # e.g. {'subjects': { 'submitter_id': 'jdoe_123' }}
                        new_entity[entity_field][keys[1]] = tsv_row[possible_tsv_fields[0]]
                    else:
                        if template_field in tsv_row:
                            if tsv_row[template_field]:
                                # template field present in tsv and tsv has value specified, set appropriately
                                if template_field in number_fields:
                                    new_entity[entity_field] = to_num(tsv_row[template_field])
                                elif template_field in array_fields:
                                    new_entity[entity_field] = to_array(tsv_row[template_field])
                                else:
                                    new_entity[entity_field] = tsv_row[template_field]
                            else:
                                # propagage null assignment
                                new_entity[entity_field] = None
                        elif template_obj[template_field]:
                            # template field not present in tsv, set default value if specified in template
                            new_entity[entity_field] = template_obj[template_field]

                new_entities.append(new_entity)
    elif file_type == 'json':
        if node_type == 'program':
            program: dict[str, any] = {
                'dbgap_accession_number': os.environ.get('PROGRAM_NAME', 'pcdc'),
                'type': 'program',
                'name': os.environ.get('PROGRAM_NAME', 'pcdc')
            }
            new_entities.append(program)
        else:
            file_path = os.path.join(local_path, f'{node_type}.json')
            if not os.path.exists(file_path):
                logger.warning("File '%s' not found, node type '%s' not loaded", file_path, node_type)
                return

            with open(file_path, encoding='utf-8') as input_file:
                records: any = json.load(input_file)

                if node_type == 'project':
                    new_entities.append(records)
                else:
                    new_entities.extend(records)
    else:
        logger.error("ERROR: Unsupported file type '%s' specified, load skipped.", file_type)
        return

    if len(new_entities) == 0:
        logger.warning("No entities retrieved from file '%s' for load", local_path)
        return

    # submit the list of items to the API endpoint
    entity: dict[str, any] = new_entities[0]
    program_name: str = None
    project_code: str = None
    response: dict[str, any] = None
    failed_submit_attempts: int = 0
    if node_type == 'program':
        try:
            response = gen3_sub.create_program(entity)
        except (requests.HTTPError, requests.ConnectionError) as exception:
            logger.error(exception)
            logger.error(entity)
            if response:
                logger.debug(response)
    elif node_type == 'project':
        try:
            if file_type == 'json':
                program_name = os.environ.get('PROGRAM_NAME', 'pcdc')
            elif file_type == 'tsv':
                program_name = entity['programs']['name']
                del entity['programs']

            response = gen3_sub.create_project(program_name, entity)
        except (requests.HTTPError, requests.ConnectionError) as exception:
            logger.error(exception)
            logger.error(entity)
            if response:
                logger.debug(response)
    else:
        if file_type == 'json':
            program_name = os.environ.get('PROGRAM_NAME', 'pcdc')
            project_code = os.environ.get('PROJECT_CODE', None)
        elif file_type == 'tsv':
            # e.g. 'pcdc-20220808'
            values: list = entity['project_id'].split('-')
            if len(values) != 2:
                raise ValueError(f'Invalid project id for entity of type {node_type}: \'{entity["project_id"]}\'')
            program_name = values[0]
            project_code = values[1]

        batch_size: int = max(int(os.environ.get('BATCH_SIZE', '100')), 1)
        i: int = 0
        max_submit_attempts: int = max(int(os.environ.get('MAX_SUBMIT_ATTEMPTS', '3')), 1)
        failed_submit_attempts = 0
        while i < len(new_entities):
            index_end: int = min(i + batch_size, len(new_entities))
            entities: list = new_entities[i:index_end]
            for entity in entities:
                entity.pop('project_id', None)

            submit_attempts: int = 0
            while submit_attempts < max_submit_attempts:
                try:
                    response = gen3_sub.submit_record(program_name, project_code, entities)
                    break
                except (requests.HTTPError, requests.ConnectionError)  as exception:
                    failed_submit_attempts += 1
                    logger.error('Error submitting entities %d => %d (attempt %d)', i, index_end, submit_attempts + 1)
                    if 0 <= i < len(new_entities) and 0 <= index_end < len(new_entities):
                        try:
                            logger.error(
                                'Error submitting entities %s => %s',
                                new_entities[i]['submitter_id'],
                                new_entities[index_end]['submitter_id']
                            )
                        finally:
                            pass
                    logger.error(exception)

                    if exception and hasattr(exception, 'response'):
                        if hasattr(exception.response, 'status_code'):
                            logger.error('Exception response status code: %s', exception.response.status_code)
                        if hasattr(exception.response, 'content'):
                            logger.error('Exception response content: %s', exception.response.content)

                        if hasattr(exception.response, 'status_code'):
                            if exception.response.status_code == 400:
                                # validation error likely
                                print('fix tsv file data')
                            if exception.response.status_code == 502:
                                # unrecoverable service error
                                raise

                    if response:
                        logger.info('Remote response:')
                        logger.info(response)

                    submit_attempts += 1
                    if submit_attempts >= max_submit_attempts:
                        logger.fatal('Max submit attempts reached, aborting load')
                        # last (re-)try attempted, note failed entities and allow exception to bubble up call stack
                        raise
                    logger.info('Pausing for %d seconds before submission re-attempt', 60 * submit_attempts)
                    time.sleep(60 * submit_attempts)

            i = index_end
            if i % 1000 == 0:
                logger.info('%d records processed', i)

        if i % 1000 != 0:
            logger.info('%d records processed', i)

    total_failed_submit_attempts += failed_submit_attempts
    msg: str = f'{node_type}: {failed_submit_attempts} failed submit attempt(s), {total_failed_submit_attempts} overall'
    if not failed_submit_attempts:
        logger.info(msg)
    else:
        logger.warning(msg)
