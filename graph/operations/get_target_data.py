"""
Retrieve external reference data from GDC
"""
import json
import os
import sys
import csv
import logging
import requests
import dotenv


def get_gdc_record(usi: str):
    """
    Retrieve GDC record for specified subject id
    """
    fields: str = ','.join(['submitter_id'])

    gdc_submitter_id_to_find: str = _CONFIG['GDC_SUBMITTER_ID_PREFIX'] + str(usi)
    filters: dict[str, any] = {
        'op': 'and',
        'content': [
            {
                'op': 'in',
                'content': {
                    'field': 'submitter_id',
                    'value': [gdc_submitter_id_to_find]
                }
            },
            {
                'op': 'in',
                'content': {
                    'field': 'project.project_id',
                    'value': [_CONFIG['GDC_PROJECT_ID']]
                }
            }
        ]
    }

    # With a GET request, the filters parameter needs to be converted
    # from a dictionary to JSON-formatted string
    params: dict[str, str] = {
        'filters': json.dumps(filters),
        'fields': fields,
        'format': 'JSON',
        'size': '10000'
    }

    logger.debug('get gdc record for %s', gdc_submitter_id_to_find)

    response: requests.Response = requests.get(_CONFIG['GDC_API_ENDPOINT'], params=params, timeout=30)

    hits = json.loads(response.content)['data']['hits']

    if len(hits) > 1:
        logger.warning('no gdc match found, review the case possible error, or in general not handled')

    return hits[0] if len(hits) == 1 else None


def build_external_resource_file(path: str, template_url: str = None):
    """
    Build (append if already exists) gen3_external_reference.tsv file for subjects found at specified path
    """
    logger.info('Building external resource file')

    if template_url is not None:
        logger.warning('template url parameter is extraneous and can be omited')

    external_reference_file_path: str = os.path.join(path, 'gen3_external_reference.tsv')
    existing_external_reference_submitter_ids: dict[str, str] = {}
    if os.path.exists(external_reference_file_path):
        with open(external_reference_file_path, mode='r', encoding='utf-8') as tsvfile:
            existing_external_references: list[dict[str, any]] = csv.DictReader(tsvfile, dialect='excel-tab')
            existing_external_reference: dict[str, any]
            for existing_external_reference in existing_external_references:
                existing_external_reference_submitter_ids[
                    existing_external_reference['*submitter_id']
                ] = existing_external_reference['*submitter_id']
    external_references: list[dict[str, any]] = []
    with open(os.path.join(path, 'gen3_subject.tsv'), mode='r', encoding='utf-8') as tsvfile:
        tsv_subjects: list[dict[str, any]] = list(csv.DictReader(tsvfile, dialect='excel-tab'))

        tsv_subjects_processed: int = 0
        tsv_subject: dict[str, any]
        for tsv_subject in tsv_subjects:
            tsv_subjects_processed += 1
            if tsv_subjects_processed % 1000 == 0:
                logger.info(
                    '%d/%d subjects processed, processing submitter_id %s)',
                    tsv_subjects_processed,
                    len(tsv_subjects),
                    tsv_subject['*submitter_id']
                )

            external_reference_submitter_id: str = f"external_reference_gdc_{tsv_subject['*submitter_id']}"

            if ((not _CONFIG.get('OVERWRITE_EXISTING_EXTERNAL_RESOURCE_FILE', False)) and
                external_reference_submitter_id in existing_external_reference_submitter_ids):
                logger.info('%s: existing external reference entry found, skipping')
                continue

            usi: list[str] =  tsv_subject['*submitter_id'].split('_', 1)
            if len(usi) != 2:
                logger.warning('malformed submitter_id: %s', tsv_subject['*submitter_id'])
                continue
            # ex: COG_PACLAX => data contributor = COG, USI = PACLAX
            gdc_record: dict[str, any] = get_gdc_record(usi[1])
            if gdc_record:
                external_obj: dict[str, any] = {}
                external_obj['type'] = 'external_reference'
                external_obj['project_id'] = tsv_subject['project_id']
                external_obj['*subjects.submitter_id'] = tsv_subject['*submitter_id']
                external_obj['external_resource_icon_path'] = _CONFIG['EXTERNAL_RESOURCE_ICON_PATH']
                external_obj['external_resource_id'] = 1
                external_obj['external_resource_name'] = _CONFIG['EXTERNAL_RESOURCE_NAME']
                external_obj['*submitter_id'] = external_reference_submitter_id

                external_obj['external_subject_url'] = _CONFIG['EXTERNAL_SUBJECT_URL_PREFIX'] + str(gdc_record['id'])
                external_obj['external_subject_id'] = str(gdc_record['id'])
                external_obj['external_subject_submitter_id'] = str(gdc_record['submitter_id'])
                external_obj['external_links'] = (
                    external_obj['external_resource_name'] + '|' +
                    external_obj['external_resource_icon_path'] + '|' +
                    external_obj['external_subject_url']
                )

                external_references.append(external_obj)

        logger.info(
            '%d subjects processed, %d external references loaded, creating/appending tsv output file',
            tsv_subjects_processed, len(external_references)
        )

    if not external_references:
        logger.warning('No external references loaded, output file not created/appended')
        return

    write_header: bool = not os.path.exists(external_reference_file_path)
    with open(
        external_reference_file_path,
        mode='w' if _CONFIG['OVERWRITE_EXISTING_EXTERNAL_RESOURCE_FILE'] else 'a',
        encoding='utf-8'
    ) as external_file:
        fieldnames: list[str] = [
            'type',
            'project_id',
            '*submitter_id',
            '*subjects.submitter_id',
            'external_resource_icon_path',
            'external_resource_id',
            'external_resource_name',
            'external_subject_id',
            'external_subject_submitter_id',
            'external_subject_url',
            'external_links'
        ]
        external_writer: csv.DictWriter = csv.DictWriter(external_file, fieldnames=fieldnames, dialect='excel-tab')
        if write_header:
            external_writer.writeheader()
        external_writer.writerows(external_references)


_CONFIG: dict[str, any] = {
    'LOG_FILE_PATH': './get_target_data.log',
    'LOG_FILE_APPEND': False,
    'GDC_API_ENDPOINT': 'https://api.gdc.cancer.gov/cases',
    'GDC_SUBMITTER_ID_PREFIX': 'TARGET-30-',
    'GDC_PROJECT_ID': 'TARGET-NBL',
    'EXTERNAL_RESOURCE_ICON_PATH': (
        'https://pcdc-external-resource-files.s3.amazonaws.com/NHI_GDC_DataPortal-logo.23e6ca47.svg'
    ),
    'EXTERNAL_SUBJECT_URL_PREFIX': 'https://portal.gdc.cancer.gov/cases/',
    'EXTERNAL_RESOURCE_NAME': 'TARGET - GDC',
    'LOCAL_FILE_PATH': dotenv.dotenv_values('../.env').get('LOCAL_FILE_PATH'),
    'OVERWRITE_EXISTING_EXTERNAL_RESOURCE_FILE': False
}

if not _CONFIG.get('LOG_FILE_APPEND', False) and os.path.exists(_CONFIG['LOG_FILE_PATH']):
    os.remove(_CONFIG['LOG_FILE_PATH'])

logging.basicConfig(
    level = logging.INFO,
    format = '%(asctime)s [%(levelname)s] %(message)s',
    handlers = [
        logging.FileHandler(_CONFIG['LOG_FILE_PATH']),
        logging.StreamHandler(sys.stdout)
    ]
)

logger: logging.Logger = logging.getLogger()

if __name__ == '__main__':
    build_external_resource_file(_CONFIG['LOCAL_FILE_PATH'])
