""" Load Elasticsearch index and switch aliases """
import json
import logging
import sys
import time

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch import exceptions


ES_BULK_BATCH_SIZE_DEFAULT: int = 1000
ES_BULK_MAX_TRIES_DEFAULT: int = 3
ES_BULK_RETRY_DELAY_DEFAULT: int = 60
ES_TIMEOUT_DEFAULT: int = 60
ES_INDEX_MAPPING_TOTAL_FIELDS_LIMIT: int = 2000
PCDC_ALIAS: str = 'pcdc'
PCDC_ARRAY_CONFIG_ALIAS: str = 'pcdc-array-config'

logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False
if len(logger.handlers) > 0:
    logger.handlers.clear()
logger.addHandler(logging.StreamHandler(sys.stdout))
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
for handler in logger.handlers:
    handler.setFormatter(formatter)


def get_es(es_port: int, es_host: str = 'localhost', es_timeout: int = ES_TIMEOUT_DEFAULT) -> Elasticsearch:
    """ Get Elasticsearch instance with specified port and host """
    return Elasticsearch([{'host': es_host, 'port': int(es_port), 'schema': 'http'}], timeout=es_timeout)


def switch_alias(es_port: int, old_index: str, new_index: str) -> None:
    """ Switch Elasticsearch alias for specified instance and index names """
    es_instance: Elasticsearch = get_es(es_port)

    logger.info('Adding new alias "%s" for index "%s', PCDC_ALIAS, new_index)
    es_instance.indices.put_alias(index=new_index, name=PCDC_ALIAS)
    logger.info('Adding new alias "%s" for index "%s', PCDC_ARRAY_CONFIG_ALIAS, new_index + '-array-config')
    es_instance.indices.put_alias(index=new_index + '-array-config', name=PCDC_ARRAY_CONFIG_ALIAS)

    try:
        logger.info('Deleting old alias "%s" for index "%s', PCDC_ALIAS, old_index)
        es_instance.indices.delete_alias(index=old_index, name=PCDC_ALIAS)
    except exceptions.NotFoundError as nferr:
        logger.error('Error deleting old alias (not found):')
        logger.error(nferr)

    try:
        logger.info('Deleting old alias "%s" for index "%s', PCDC_ARRAY_CONFIG_ALIAS, old_index + '-array-config')
        es_instance.indices.delete_alias(index=old_index + '-array-config', name=PCDC_ARRAY_CONFIG_ALIAS)
    except exceptions.NotFoundError as nferr:
        logger.error('Error deleting old alias:')
        logger.error(nferr)


def load_es_array_config_index(es_instance: Elasticsearch, index_name: str) -> None:
    """ Load Elasticsearch array config index for specified index_name => '{index_name}-array-config' """
    logger.info('Loading ES array config index %s', index_name)
    mapping: dict[str, any] = {
        'mappings': {
            'properties': {
                'array': {
                    'type' : 'keyword'
                },
                'timestamp': {
                    'type': 'date'
                }
            }
        }
    }

    doc: dict[str, any] = {
        'timestamp': '2021-04-29T16:56:06.490549',
        'array': [
            'adverse_events',
            'biopsy_surgical_procedures',
            'biospecimens',
            'cytologies',
            'disease_characteristics',
            'external_references',
            'function_tests',
            'histologies',
            'imagings',
            'labs',
            'lesion_characteristics',
            'medical_histories',
            'minimal_residual_diseases',
            'molecular_analysis',
            'myeloid_sarcoma_involvements',
            'non_protocol_therapies',
            'off_protocol_therapy_studies',
            'radiation_therapies',
            'secondary_malignant_neoplasm',
            'stagings',
            'stem_cell_transplants',
            'studies',
            'studies.treatment_arm',
            'subject_responses',
            'survival_characteristics',
            'timings',
            'total_doses',
            'transfusion_medicine_procedures',
            'tumor_assessments',
            'vitals'
        ]
    }

    request_body: dict[str, any] = {'settings': {'number_of_shards': 1, 'number_of_replicas': 1}}
    request_body.update(mapping)
    index: str = f'{index_name}-array-config'
    es_instance.indices.create(index=index, body=request_body, include_type_name=False)
    es_instance.index(index, id=PCDC_ALIAS, body=doc)
    logger.info('Loaded ES array config index')


def try_bulk(
    es_instance: Elasticsearch,
    bulk_actions: list,
    es_bulk_max_tries: int = ES_BULK_MAX_TRIES_DEFAULT,
    es_bulk_retry_delay: int = ES_BULK_RETRY_DELAY_DEFAULT,
    es_timeout: any = ES_TIMEOUT_DEFAULT
) -> None:
    """ Attempt to perform bulk actions for specified ES instance and action list """
    tries: int = 0
    while tries < max(es_bulk_max_tries, 1):
        tries += 1
        try:
            helpers.bulk(es_instance, bulk_actions, request_timeout=es_timeout)
            break
        except (exceptions.TransportError, exceptions.RequestError, exceptions.ConnectionError) as err:
            if tries >= es_bulk_max_tries:
                logger.error('Error performing bulk operation, max tries (%d) attempted', es_bulk_max_tries)
                raise
            logger.error(
                'Error performing bulk operation (attempt #%d), retrying after %d seconds:', tries, es_bulk_retry_delay
            )
            logger.error(err)
            time.sleep(es_bulk_retry_delay)


def load_es_data_index(
    es_instance: Elasticsearch,
    data: list,
    index_name: str,
    es_bulk_batch_size: int = ES_BULK_BATCH_SIZE_DEFAULT,
    es_bulk_max_tries: int = ES_BULK_MAX_TRIES_DEFAULT,
    es_bulk_retry_delay: int = ES_BULK_RETRY_DELAY_DEFAULT,
    es_timeout: int = ES_TIMEOUT_DEFAULT
) -> None:
    """
    Load ES index for specified instance, index, and json data. Optional parameters can be specified for ES bulk
    API call batch size, max tries on exception, and delay between tries.
    """
    logger.info('Loading ES data index %s', index_name)
    # load field mapping
    mapping: dict[str, any]
    with open('../files/nested_mapping.json', encoding='utf-8') as mapping_f:
        mapping = json.load(mapping_f)

    # data to be loaded
    index: str = index_name
    docs: list = data

    # create ES index and assign mapping
    request_body: dict[str, any] = {
        'settings' : {
            'number_of_shards': 1,
            'number_of_replicas': 1,
            'index.mapping.total_fields.limit': ES_INDEX_MAPPING_TOTAL_FIELDS_LIMIT
        }
    }
    request_body.update(mapping)
    es_instance.indices.create(index=index, body=request_body, include_type_name=False)

    # load data
    i: int = 0
    doc: dict[str, any]
    bulk_actions: list[any] = []
    for doc in docs:
        i += 1
        doc_id: str = 'subj_' + str(i)
        bulk_actions.append({'_index': index_name, '_id': doc_id, '_source': doc})

        if len(bulk_actions) % es_bulk_batch_size == 0:
            try_bulk(es_instance, bulk_actions, es_bulk_max_tries, es_bulk_retry_delay, es_timeout)
            bulk_actions.clear()
            logger.info('Loaded %d of %d records into index "%s"', i, len(docs), index_name)

    if bulk_actions:
        try_bulk(es_instance, bulk_actions, es_bulk_max_tries, es_bulk_retry_delay)
        bulk_actions.clear()
        logger.info('Loaded %d records into index "%s"', i, index_name)

    logger.info('Loaded ES data index')


def load_es_data(
    data: list,
    es_port: int,
    index_name: str,
    es_bulk_batch_size: int = ES_BULK_BATCH_SIZE_DEFAULT,
    es_bulk_max_tries: int = ES_BULK_MAX_TRIES_DEFAULT,
    es_bulk_retry_delay: int = ES_BULK_RETRY_DELAY_DEFAULT,
    es_timeout: int = ES_TIMEOUT_DEFAULT
) -> None:
    """
    'Public'-facing function to load ES data index for specified json data set, ES port and index name. Optional
    parameters can be specified for ES bulk API call batch size, max tries on exception, and delay between tries.
    """
    es_instance: Elasticsearch = get_es(es_port)
    load_es_data_index(
        es_instance,
        data,
        index_name,
        es_bulk_batch_size,
        es_bulk_max_tries,
        es_bulk_retry_delay,
        es_timeout
    )


def load_es_array_config(es_port: int, index_name: str) -> None:
    """ Load Elasticsearch data and array config indexes for specified json data set, ES port and index name """
    es_instance: Elasticsearch = get_es(es_port)
    load_es_array_config_index(es_instance, index_name)
