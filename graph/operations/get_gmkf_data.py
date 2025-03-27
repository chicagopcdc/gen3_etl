"""
Retrieve external data from GMKF
"""
import collections
import json
import logging
import io
import os
import sys
import csv
from urllib.parse import parse_qs, urlencode, urljoin, urlsplit

import requests
import dotenv


def normalize_gmkf_url(orig_url: str, gmkf_returned_url: str) -> str:
    """
    URLs e.g. for paged search results, come back from GMKF API with host set to localhost, e.g.
    http://localhost:8000?_getpages=5e5c3a52-8032-4aee-a424-c5891b0d818a&_getpagesoffset=50&_co...etc
    instead of http://[gmkf_api_hostname]?_getpages=etc. This method will correct the hostname by
    parsing (safer than find/replace) to split URL into component parts and correct 'netloc'
    (aka host) with correct hostname as specified in original URL.
    """
    normalized_url: str = gmkf_returned_url
    # {scheme}://{netloc}/{path}?{query}#{fragment}
    orig_url_parts: collections.namedtuple = urlsplit(orig_url)
    gmkf_url_parts: collections.namedtuple = urlsplit(gmkf_returned_url)
    if 'localhost' in gmkf_url_parts[1]:
        return gmkf_url_parts._replace(netloc=orig_url_parts.netloc).geturl()
    return normalized_url


def is_number(value: str):
    """ Determine whether specified string is number (float or int) """
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def get_subject_external_participant_id(subject: dict[str, any]) -> str:
    """
    get external participant id ('GMKF-30-{USI}NN' format e.g. 'GMKF-30-ABCDEF03') for specified fhir (json) subject
    """
    identifier: dict[str, any]
    for identifier in subject['resource']['identifier']:
        if identifier['use'] == 'secondary':
            return identifier['value']

    return None


def get_external_participant_id_usi(external_participant_id: str) -> str:
    """ get USI for specified external participant id ('GMKF-30-{USI}NN' format e.g. 'GMKF-30-ABCDEF03' => ABCDEF) """
    if '-' not in external_participant_id:
        return None
    usi: str = external_participant_id.split(_CONFIG.get('GMKF_SUBMITTER_ID_PREFIX', '-'))[-1]
    suffix: str = usi[-2:]
    return usi[:-2] if is_number(suffix) and float(suffix).is_integer() else None


def get_external_participant_id_index(external_participant_id: str) -> int:
    """ get index for specified external participant id ('GMKF-30-{USI}NN' format e.g. 'GMKF-30-ABCDEF03' => 03) """
    if '-' not in external_participant_id:
        return None
    usi: str = external_participant_id.split('-')[-1]
    suffix: str = usi[-2:]
    return int(float(suffix)) if is_number(suffix) and float(suffix).is_integer() else None


def get_subjects_by_study_id(study_id: str, url: str, request_cookies: dict[str, str] = None) -> dict[str, any]:
    """
    get gmkf fhir resource(s) for specified study and url
    """
    lowest_usi_external_participant_ids: dict[str, str] = {}
    subjects: dict[str, dict[str, any]] = {}
    subject_usi_external_participant_ids: dict[str, str] = {}

    params: dict[str, str] = {'study': study_id}
    # ex: https://fhir.kidsfirstdrc.org/ResearchSubject?study=sd-dypmehhf
    response: requests.Response = requests.get(url, params=params, timeout=30, cookies=request_cookies)
    json_data: dict[str, any] = json.loads(response.content)

    external_participant_id: str
    usi: str

    total_entries: int = json_data['total']
    entries_processed: int = 0
    while True:
        # enumerate entries returned by GMKF API and then follow 'link' if populated
        logger.info(
            'Processing %d => %d of %d remote subject entries',
            entries_processed + 1,
            entries_processed + len(json_data['entry']),
            total_entries
        )
        entries_processed += len(json_data['entry'])
        entry: dict[str, any]
        for entry in json_data['entry']:
            external_participant_id = get_subject_external_participant_id(entry)
            if not external_participant_id:
                logger.warning('No external participant id (secondary identity) found for subject entry: %s', entry)
                continue

            usi = get_external_participant_id_usi(external_participant_id)
            if not usi:
                logger.warning('No USI found for subject entry: %s', entry)
                continue

            external_participant_id_index: int = get_external_participant_id_index(external_participant_id)
            if external_participant_id_index < lowest_usi_external_participant_ids.get(usi, sys.maxsize):
                lowest_usi_external_participant_ids[usi] = external_participant_id_index
                subjects[usi] = entry
                subject_usi_external_participant_ids[usi] = external_participant_id

		# 'link' property will be populated with a follow-up URL for paged results
        next_page_url: str = None
        link: dict[str, any]
        for link in [l for l in json_data['link'] if l['relation'] == 'next']:
            # paged response
            next_page_url = link['url']
            break

        if not next_page_url:
            break

        response = requests.get(next_page_url, timeout=30, cookies=request_cookies)
        json_data = json.loads(response.content)

    for usi, external_participant_id in subject_usi_external_participant_ids.items():
        if get_external_participant_id_index(external_participant_id) != 3:
            raise RuntimeError(f'Unexpected external participant id for USI {usi}: {external_participant_id}')

    logger.info('%d subjects with unique USIs found for %d total remote subjects', len(subjects), total_entries)

    return subjects


def get_study_id_by_title(study_title: str, study_url: str, request_cookies: dict[str, str] = None) -> str:
    """
    get id of gmkf study with specified title
    """
    # ex: https://fhir.kidsfirstdrc.org/ResearchStudy?title=Discovering the Genetic Basis...etc...
    # {scheme}://{netloc}/{path}?{query}#{fragment}
    url_parts: collections.namedtuple = urlsplit(study_url)
    query: dict[str, str] = parse_qs(url_parts.query)
    query.update({'title': study_title})
    url: str = url_parts._replace(query=urlencode(query)).geturl()

    err_msg: str
    try:
        response: requests.Response = requests.get(url, timeout=30, cookies=request_cookies)
        if not response.ok:
            response.raise_for_status()
        json_data: dict[str, any] = json.loads(response.content)

        studies: list[dict[str, any]] = json_data.get('entry', [])
        if len(studies) != 1:
            err_msg = f'ERROR: {len(studies)} studies returned with title {study_title}'
            logger.critical(err_msg)
            logger.critical(json_data)
            raise RuntimeError(err_msg)

        if studies[0]['resource'] and studies[0]['resource']['id']:
            return studies[0]['resource']['id']

        err_msg = 'ERROR: Unable to get study id, check the response body:'
        logger.critical(err_msg)
        logger.critical(json_data)
        raise RuntimeError(err_msg)
    except requests.exceptions.HTTPError as http_error:
        logger.error('HTTP error retrieving getting study id by title:')
        logger.exception(http_error)
        raise
    except json.decoder.JSONDecodeError as json_decode_error:
        logger.error('JSON decode error getting study id by title:')
        logger.exception(json_decode_error)
        raise
    except Exception as err:
        logger.error('Error getting study id by title:')
        logger.exception(err)
        raise


def get_subject_by_subject_id(subject_id: str, base_url: str, request_cookies: dict[str, str] = None) -> dict[str, any]:
    """
    get gmkf subject for specified id
    """
    # ex: https://fhir.kidsfirstdrc.org/ResearchSubject/rs-sab7ybcx
    subject_url: str = urljoin(base_url, subject_id)

    logger.info('get_subject_by_subject_id url: %s', subject_url)
    response: requests.Response = requests.get(subject_url, timeout=30, cookies=request_cookies)
    return json.loads(response.content)


def get_subjects_from_file(file_path: str) -> dict[str, any]:
    """
    get gmkf subjects from specified file
    """
    if not os.path.isfile(file_path):
        raise RuntimeError(f'Unable to load subjects from path: "{file_path}"')
    csv_fd: io.TextIOWrapper
    csv_reader: csv.DictReader
    with open(file_path, 'r', encoding='utf-8') as csv_fd:
        csv_reader = csv.DictReader(csv_fd)
        subjects: list[dict[str, any]] = list(csv_reader)
        return {s['cog_usi']:s for s in subjects}


def build_external_resource_file(path: str, gmkf_subjects: dict[str, any]):
    """
    Build (append if already exists) gen3_external_reference.tsv file for subjects found at specified path
    """
    logger.info('Building external resource file')

    external_reference_file_path: str = os.path.join(path, 'gen3_external_reference.tsv')
    existing_external_reference_submitter_ids: dict[str, str] = {}
    if os.path.exists(external_reference_file_path):
        with open(external_reference_file_path, mode='r', encoding='utf-8') as tsvfile:
            existing_external_references: list[dict[str, any]] = csv.DictReader(tsvfile, dialect='excel-tab')
            existing_external_reference: dict[str, any]
            for existing_external_reference in existing_external_references:
                existing_external_reference_submitter_ids[existing_external_reference['*submitter_id']] = \
                    existing_external_reference['*submitter_id']
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

            external_reference_submitter_id: str = f'external_reference_gmkf_{tsv_subject["*submitter_id"]}'

            # if we don't want to overwrite an existing external resource file (appending if exists)
            # then we also don't want to overwrite/update existing records in the file either
            # so skip if there's an existing file and this record is present there.
            if (
                (not _CONFIG.get('OVERWRITE_EXISTING_EXTERNAL_RESOURCE_FILE', False))
                and
                external_reference_submitter_id in existing_external_reference_submitter_ids
            ):
                logger.info('%s: existing external reference entry found, skipping', external_reference_submitter_id)
                continue

            gmkf_submitter_id: str = tsv_subject['*honest_broker_subject_id']
            if not gmkf_subjects.get(gmkf_submitter_id):
                continue

            external_obj: dict[str, any] = {}
            external_obj['type'] = 'external_reference'
            external_obj['project_id'] = tsv_subject['project_id']
            external_obj['*subjects.submitter_id'] = tsv_subject['*submitter_id']
            external_obj['external_resource_icon_path'] = _CONFIG['EXTERNAL_RESOURCE_ICON_PATH']
            external_obj['external_resource_id'] = 2
            external_obj['external_resource_name'] = _CONFIG['EXTERNAL_RESOURCE_NAME']
            external_obj['*submitter_id'] = external_reference_submitter_id

            # determine whether our source data was retrieved from flat file or API
            if gmkf_subjects[gmkf_submitter_id].get('resource', {}).get('identifier'):
                # API-sourced subject record
                identifier: dict[str, any]
                for identifier in gmkf_subjects[gmkf_submitter_id]['resource']['identifier']:
                    if identifier.get('system') == _CONFIG['RESOURCE_ID_SYSTEM_PARTICIPANTS_URL']:
                        external_obj['external_subject_submitter_id'] = str(identifier['value'])
                        external_obj['external_subject_url'] = (
                            _CONFIG['EXTERNAL_SUBJECT_URL_PREFIX'] + external_obj['external_subject_submitter_id']
                        )
                    elif identifier.get('system') == _CONFIG['RESOURCE_ID_SYSTEM_UNIQUE_STRING_URN']:
                        external_obj['external_subject_id'] = str(identifier['value'])
                    else:
                        logger.warning(
                            '"external_subject_submitter_id" and "external_subject_id" not populated for subject: %s',
                            tsv_subject['*submitter_id']
                        )
            else:
                # file-sourced subject record
                external_obj['external_subject_submitter_id'] = gmkf_subjects[gmkf_submitter_id]['kf_participant_id']
                external_obj['external_subject_url'] = (
                    _CONFIG['EXTERNAL_SUBJECT_URL_PREFIX'] + external_obj['external_subject_submitter_id']
                )
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
        logger.warning('No new/updated external references loaded, tsv output file not created/appended')
        return

    write_header: bool = not os.path.exists(external_reference_file_path)
    with open(
        external_reference_file_path,
        mode='w' if _CONFIG.get('OVERWRITE_EXISTING_EXTERNAL_RESOURCE_FILE', False) else 'a',
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


def main():
    """
    Standalone entry point
    """
    subjects: dict[str, any] = {}
    if _CONFIG.get('GMKF_SUBJECT_FILE_PATH'):
        logger.info('Building external resource file using source file %s', _CONFIG['GMKF_SUBJECT_FILE_PATH'])
        subjects: dict[str, any] = get_subjects_from_file(_CONFIG['GMKF_SUBJECT_FILE_PATH'])
        if subjects:
            logger.info('Building external resource file for %d subjects', len(subjects))
            build_external_resource_file(_CONFIG['LOCAL_FILE_PATH'], subjects)
        else:
            logger.warning(
                'No subjects found for subjects in "%s", external resource file not built',
                _CONFIG['LOCAL_FILE_PATH']
            )
    else:
        logger.info('Building external resource file using FHIR API')
        cookies: dict[str, str] = {'arc-user': _CONFIG['GMKF_AUTH_ARC_USER']}
        study_id: str = get_study_id_by_title(_CONFIG['GMKF_STUDY_TITLE_NBL'], _CONFIG['GMKF_STUDY_URL'], cookies)
        studies: dict[str, str] = {study_id: _CONFIG['GMKF_STUDY_TITLE_NBL']}
        subjects: dict[str, any] = {}
        for study_id, study_title in studies.items():
            study_subjects = get_subjects_by_study_id(study_id, _CONFIG['GMKF_SUBJECT_URL'], cookies)
            logger.info('%d subjects found for study "%s" ("%s")', len(study_subjects), study_id, study_title)
            subjects = {**subjects, **study_subjects}
        if subjects:
            logger.info('Building external resource file for %d subjects', len(subjects))
            build_external_resource_file(_CONFIG['LOCAL_FILE_PATH'], subjects)
        else:
            logger.warning('No subjects found for study "%s", external resource file not built', study_id)


_CONFIG: dict[str, any] = {
    'LOG_FILE_PATH': './get_gmkf_data.log',
    'LOG_FILE_APPEND': False,
    # credentials for FHIR API; log in to e.g. https://fhir.kidsfirstdrc.org/ResearchSubject?study=sd-dypmehhf, find
    # cookie 'arc-user' using browser dev tools, then save value to GMKF_AUTH_ARC_USER config var in ../.env
    'GMKF_AUTH_ARC_USER': '',
    # file source will be used if path specified; leave blank/null to use API
    #'GMKF_SUBJECT_FILE_PATH': '/Users/schoi/Workspace/PED/PCDC/Projects/_data/gen3_etl/gmkf/nbl-cog-usis.csv',
    'GMKF_SUBJECT_FILE_PATH': '',
    'GMKF_SUBMITTER_ID_PREFIX': 'GMKF-30-',
    'GMKF_STUDY_URL': 'https://fhir.kidsfirstdrc.org/ResearchStudy',
    'GMKF_SUBJECT_URL': 'https://fhir.kidsfirstdrc.org/ResearchSubject',
    'GMKF_STUDY_TITLE_NBL': (
        'Discovering the Genetic Basis of Human Neuroblastoma: A Gabriella Miller Kids First Pediatric Research ' +
        'Program (Kids First) Project'
    ),
    'GMKF_STUDY_TITLE_NBL_OLD': 'TARGET: Neuroblastoma (NBL)',
    'EXTERNAL_RESOURCE_ICON_PATH': (
        'https://pcdc-external-resource-files.s3.us-east-1.amazonaws.com/' +
            'Kids_First_Graphic_Horizontal_OL_FINAL.DRC-01-scaled.png'
    ),
    'RESOURCE_ID_SYSTEM_PARTICIPANTS_URL': 'https://kf-api-dataservice.kidsfirstdrc.org//participants/',
    'RESOURCE_ID_SYSTEM_UNIQUE_STRING_URN': 'urn:kids-first:unique-string',
    'EXTERNAL_SUBJECT_URL_PREFIX': 'https://portal.kidsfirstdrc.org/participants/',
    'EXTERNAL_RESOURCE_NAME': 'GMKF',
    'LOCAL_FILE_PATH': '',
    'OVERWRITE_EXISTING_EXTERNAL_RESOURCE_FILE': False
}
# override config defaults (or set FHIR API auth; see comments above) using .env config file in parent directory
_env_vals: dict[str, str] = dotenv.dotenv_values('../.env')
_CONFIG.update(_env_vals)

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
    main()
    #do_test()
