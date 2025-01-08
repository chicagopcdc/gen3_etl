"""
Retrieve external reference data from CDS
"""
import json
import os
import sys
import csv
import logging
import requests
import dotenv


def get_cds_record(usi_list: list[str]):
    """
    Retrieve CDS record for specified subject id
    {
      findSubjectIdsInList(subject_ids: ["FOOBAR", "1"]) {
        subject_id
        phs_accession
         
      }
      participant(participant_id: "FOOBAR"){
        dbGaP_subject_id
        gender
        participant_id
        study {
          phs_accession
        }
      }
    }
    """
    

    query = "{{findSubjectIdsInList( subject_ids: {idList}){{ subject_id  phs_accession }}}}".format(idList=json.dumps(usi_list))
    

    # With a GET request, the filters parameter needs to be converted
    # from a dictionary to JSON-formatted string
    params: dict[str, str] = {
        'query': query,
        'variables': None
    }

    logger.debug('getting cds record for %s', usi_list)

    response: requests.Response = requests.post(_CONFIG['API_ENDPOINT'], json=params, timeout=30)
    if response.status_code != 200:
        print(response.text)
        exit()

    response = response.json()
    response = response["data"]
    subjects_in_common = response["findSubjectIdsInList"]

    if len(subjects_in_common) < 1:
        logger.warning('No subjects in common found!')

    return subjects_in_common


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
        tsv_subjects: list[dict[str, any]] = csv.DictReader(tsvfile, dialect='excel-tab')

        tsv_subjects_processed: int = 0
        tsv_subject: dict[str, any]
        for tsv_subject in tsv_subjects:
            tsv_subjects_processed += 1
            if tsv_subjects_processed % 1000 == 0:
                logger.info(
                    '%d subjects processed, processing submitter_id %s)',
                    tsv_subjects_processed, tsv_subject['*submitter_id']
                )

            external_reference_submitter_id: str = f"external_reference_cds_{tsv_subject['*submitter_id']}"

            if ((not _CONFIG.get('OVERWRITE_EXISTING_EXTERNAL_RESOURCE_FILE', False)) and
                external_reference_submitter_id in existing_external_reference_submitter_ids):
                logger.info('%s: existing external reference entry found, skipping')
                continue

            #TODO could use honest broker subject id, and check for data contributor to be COG or COG and others if anyone else is using USI
            usi: list[str] =  tsv_subject['*submitter_id'].split('_', 1)
            if len(usi) != 2:
                logger.warning('malformed submitter_id: %s', tsv_subject['*submitter_id'])
                continue
            if usi[0] != 'COG':
                continue
            # ex: COG_PACLAX => data contributor = COG, USI = PACLAX
            #TODO refactor to pass a list of USI iinstead of one, the CDS API supports that
            cds_record: dict[str, any] = get_cds_record([usi[1]])
            if cds_record:
                cds_record = cds_record[0]
                external_obj: dict[str, any] = {}
                external_obj['type'] = 'external_reference'
                external_obj['project_id'] = tsv_subject['project_id']
                external_obj['*subjects.submitter_id'] = tsv_subject['*submitter_id']
                external_obj['external_resource_icon_path'] = _CONFIG['EXTERNAL_RESOURCE_ICON_PATH']
                external_obj['external_resource_id'] = 3 #TODO check this
                external_obj['external_resource_name'] = _CONFIG['EXTERNAL_RESOURCE_NAME']
                external_obj['*submitter_id'] = external_reference_submitter_id

                external_obj['external_subject_id'] = str(cds_record['subject_id'])
                external_obj['external_subject_submitter_id'] = str(cds_record['subject_id'])

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
    'LOG_FILE_PATH': './get_cds_data.log',
    'LOG_FILE_APPEND': False,
    'API_ENDPOINT': 'https://dataservice.datacommons.cancer.gov/v1/graphql/',
    'SUBMITTER_ID_PREFIX': '',
    'PROJECT_ID': 'TARGET-NBL',
    'EXTERNAL_RESOURCE_ICON_PATH': (
        'https://raw.githubusercontent.com/CBIIT/datacommons-assets/main/cds/logo/cds-logo.svg'
    ),
    'EXTERNAL_SUBJECT_URL_PREFIX': 'https://dataservice.datacommons.cancer.gov/#/data',
    'EXTERNAL_RESOURCE_NAME': 'CDS',
    'LOCAL_FILE_PATH': dotenv.dotenv_values('../.env')['LOCAL_FILE_PATH'],
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
