""" Load Elasticsearch index and switch aliases """
import json
import logging
import os
import sys
import time

from elasticsearch import Elasticsearch
from elasticsearch import helpers
from elasticsearch import exceptions
from spark_utils import get_spark_session


ES_BULK_BATCH_SIZE_DEFAULT: int = 1000
ES_BULK_MAX_TRIES_DEFAULT: int = 3
ES_BULK_RETRY_DELAY_DEFAULT: int = 60
ES_TIMEOUT_DEFAULT: int = 60
ES_INDEX_MAPPING_TOTAL_FIELDS_LIMIT: int = 2000
ARRAY_CONFIG_ALIAS_SUFFIX: str = '-array-config'
MAPPING_FILE_DEFAULT: str = os.path.join(os.path.dirname(__file__), '..', 'files', 'nested_mapping.json')
mapping_file: str = os.environ.get('MAPPING_FILE', MAPPING_FILE_DEFAULT)

logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False
if len(logger.handlers) > 0:
    logger.handlers.clear()
logger.addHandler(logging.StreamHandler(sys.stdout))
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
for handler in logger.handlers:
    handler.setFormatter(formatter)


def get_es(es_port: int, es_host: str = 'localhost', es_timeout: int = ES_TIMEOUT_DEFAULT, es_scheme: str = 'http') -> Elasticsearch:
    """ Get Elasticsearch instance with specified port, host, and scheme """
    return Elasticsearch([{'host': es_host, 'port': int(es_port), 'schema': es_scheme}], timeout=es_timeout)


def switch_alias(es_port: int, alias: str, old_index: str, new_index: str, es_host: str = 'localhost', es_scheme: str = 'http') -> None:
    """ Switch Elasticsearch alias for specified instance and index names """
    es_instance: Elasticsearch = get_es(es_port, es_host, es_scheme=es_scheme)

    alias_array_config: str = f'{alias}{ARRAY_CONFIG_ALIAS_SUFFIX}'
    old_index_array_config: str = f'{old_index}{ARRAY_CONFIG_ALIAS_SUFFIX}'
    new_index_array_config: str = f'{new_index}{ARRAY_CONFIG_ALIAS_SUFFIX}'

    logger.info('Adding new alias "%s" for index "%s', alias, new_index)
    es_instance.indices.put_alias(index=new_index, name=alias)
    logger.info('Adding new alias "%s" for index "%s', alias_array_config, new_index_array_config)
    es_instance.indices.put_alias(index=new_index_array_config, name=alias_array_config)

    try:
        logger.info('Deleting old alias "%s" for index "%s', alias, old_index)
        es_instance.indices.delete_alias(index=old_index, name=alias)
    except exceptions.NotFoundError as nferr:
        logger.error('Error deleting old alias (not found):')
        logger.error(nferr)

    try:
        logger.info('Deleting old alias "%s" for index "%s', alias_array_config, old_index_array_config)
        es_instance.indices.delete_alias(index=old_index_array_config, name=alias_array_config)
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
            'growing_teratoma_syndromes',
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
    es_instance.index(index, id=index, body=doc)
    logger.info('Loaded ES array config index')


def _load_batch(item: dict[str, any]) -> None:
    """
    Bulk-load a single batch of documents into Elasticsearch. Runs as a Spark task,
    possibly on a remote executor, so it builds its own Elasticsearch client per task
    (a client wraps a connection pool that isn't safe to share across concurrent tasks)
    rather than reusing the driver's, and does its own logging setup rather than
    referencing the driver's module-level logger.
    """
    import logging
    from elasticsearch import Elasticsearch
    task_logger: logging.Logger = logging.getLogger(__name__)

    es_instance: Elasticsearch = Elasticsearch(
        [{'host': item['es_host'], 'port': int(item['es_port']), 'schema': item['es_scheme']}],
        timeout=item['es_timeout']
    )
    try_bulk(es_instance, item['bulk_actions'], item['max_tries'], item['retry_delay'], item['es_timeout'])
    task_logger.info(
        'Loaded batch %d/%d (%d records) into index "%s"',
        item['batch_num'], item['batch_count'], len(item['bulk_actions']), item['index_name']
    )


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
    es_host: str = 'localhost',
    es_port: int = 9200,
    es_bulk_batch_size: int = ES_BULK_BATCH_SIZE_DEFAULT,
    es_bulk_max_tries: int = ES_BULK_MAX_TRIES_DEFAULT,
    es_bulk_retry_delay: int = ES_BULK_RETRY_DELAY_DEFAULT,
    es_timeout: int = ES_TIMEOUT_DEFAULT,
    es_scheme: str = 'http'
) -> None:
    """
    Load ES index for specified instance, index, and json data. Creates the index once (via
    es_instance, on the driver), then distributes the bulk-write batches across a Spark cluster
    (es_host/es_port/es_scheme are passed separately so each task can build its own client).
    """
    logger.info('Loading ES data index %s', index_name)
    # load field mapping
    mapping: dict[str, any]
    with open(mapping_file, encoding='utf-8') as mapping_f:
        mapping = json.load(mapping_f)

    # data to be loaded
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
    es_instance.indices.create(index=index_name, body=request_body, include_type_name=False)

    if not docs:
        logger.info('Loaded ES data index')
        return

    bulk_actions: list[any] = [
        {'_index': index_name, '_id': f'subj_{i}', '_source': doc}
        for i, doc in enumerate(docs, start=1)
    ]
    batches: list[list[any]] = [
        bulk_actions[i:i + es_bulk_batch_size] for i in range(0, len(bulk_actions), es_bulk_batch_size)
    ]
    work_items: list[dict[str, any]] = [
        {
            'bulk_actions': batch,
            'es_host': es_host,
            'es_port': es_port,
            'es_scheme': es_scheme,
            'es_timeout': es_timeout,
            'max_tries': es_bulk_max_tries,
            'retry_delay': es_bulk_retry_delay,
            'index_name': index_name,
            'batch_num': batch_num,
            'batch_count': len(batches),
        }
        for batch_num, batch in enumerate(batches, start=1)
    ]

    spark = get_spark_session()
    spark.sparkContext.parallelize(work_items, numSlices=len(work_items)).foreach(_load_batch)

    logger.info('Loaded %d records into index "%s"', len(docs), index_name)


def load_es_data(
    data: list,
    es_port: int,
    index_name: str,
    es_host: str = 'localhost',
    es_bulk_batch_size: int = ES_BULK_BATCH_SIZE_DEFAULT,
    es_bulk_max_tries: int = ES_BULK_MAX_TRIES_DEFAULT,
    es_bulk_retry_delay: int = ES_BULK_RETRY_DELAY_DEFAULT,
    es_timeout: int = ES_TIMEOUT_DEFAULT,
    es_scheme: str = 'http'
) -> None:
    """
    'Public'-facing function to load ES data index for specified json data set, ES host/port and index name.
    Optional parameters can be specified for ES bulk API call batch size, max tries on exception, and delay
    between tries.
    """
    es_instance: Elasticsearch = get_es(es_port, es_host, es_scheme=es_scheme)
    load_es_data_index(
        es_instance,
        data,
        index_name,
        es_host,
        es_port,
        es_bulk_batch_size,
        es_bulk_max_tries,
        es_bulk_retry_delay,
        es_timeout,
        es_scheme
    )


def load_es_array_config(es_port: int, index_name: str, es_host: str = 'localhost', es_scheme: str = 'http') -> None:
    """ Load Elasticsearch data and array config indexes for specified json data set, ES host/port and index name """
    es_instance: Elasticsearch = get_es(es_port, es_host, es_scheme=es_scheme)
    load_es_array_config_index(es_instance, index_name)
