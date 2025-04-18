"""
Retrieve external data from GMKF
"""
import ast
import csv
import io
import json
import logging
import os
from pathlib import Path
import sys
import typing

import dotenv
import requests


def is_number(value: str) -> bool:
    """ Determine whether specified string is number (float or int) """
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def get_all_files(root_path: str, skip_paths: list[str] = None, log_skipped_files: bool = True) -> list[str]:
    """ Get list of all file paths within specified root path with optional list of path(s) to skip/ignore """
    if not root_path or not os.path.isdir(root_path):
        raise RuntimeError(f'Root path not specified or invalid dir: "{root_path}"')

    all_files: list[str] = []
    dir_path: str
    file_names: list[str]
    # os.walk is fully recursive
    for dir_path, _, file_names in os.walk(root_path):
        dir_files: list[str] = [os.path.join(dir_path, f) for f in file_names]
        skip_path: str
        for skip_path in (skip_paths or []):
            if skip_path and skip_path in dir_path[len(root_path) - 1:]:
                ignore_files: list[str] = [f for f in dir_files if any(p for p in skip_paths if p in f)]
                if log_skipped_files:
                    ignore_file: str
                    for ignore_file in ignore_files:
                        _logger.info('Skipping "%s" per config', ignore_file)
                dir_files = [f for f in dir_files if f not in ignore_files]

        all_files.extend(dir_files)
    all_files.sort()
    return all_files


def get_all_subject_files(root_path: str, skip_paths: list[str] = None, log_skipped_files: bool = True) -> list[str]:
    """
    Get list of all subject (gen3_subject.tsv) file paths within specified
    root path with optional list of path(s) to skip/ignore
    """
    subject_file_paths: list[str] = []

    all_subject_file_paths: list[str] = [f for f in get_all_files(root_path) if f.endswith('/gen3_subject.tsv')]
    subject_file_path: str
    for subject_file_path in all_subject_file_paths:
        skip_path: str
        for skip_path in (skip_paths or []):
            if skip_path and skip_path in subject_file_path[len(root_path) - 1:]:
                if log_skipped_files:
                    _logger.info('Skipping "%s" per config', subject_file_path)
        if not any(sp and sp in subject_file_path[len(root_path) - 1:] for sp in skip_paths):
            subject_file_paths.append(subject_file_path)

    return subject_file_paths


def get_gen3_subjects(gen3_subject_tsv_file_path: str) -> dict[dict[str, any]]:
    """ Load and return collection of Gen3 subject records from specified file path (gen3_subject.tsv) """
    _logger.info('Loading Gen3 subjects from "%s"', gen3_subject_tsv_file_path)
    fd_subjects: typing.TextIO
    gen3_subject_tsv_file_path: str
    subjects: dict[str, dict[str, any]] = {}
    with open(gen3_subject_tsv_file_path, 'r', encoding='utf-8') as fd_subjects:
        reader: csv.DictReader = csv.DictReader(fd_subjects, delimiter='\t')
        record: dict[str, any]
        for record in reader:
            if record['*submitter_id'] in subjects:
                _logger.warning('Subject "%s" loaded more than once')
            subjects[record['*submitter_id']] = record
    _logger.info('Loaded %d Gen3 subject records', len(subjects))
    return subjects


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


def get_json_from_url(
    url: str,
    timeout: int = 30,
    cookies: dict[str, str] = None,
    params: dict[str, str] = None
) -> any:
    """ get json content data as python object from specified url """
    try:
        response: requests.Response = requests.get(url, timeout=timeout, cookies=cookies, params=params)
        response.raise_for_status()
        return json.loads(response.content)
    except requests.exceptions.HTTPError as http_error:
        _logger.error('HTTP error retrieving JSON content from URL:')
        _logger.exception(http_error)
        if cookies:
            _logger.error('Verify valid auth token in cookie')
        raise
    except json.decoder.JSONDecodeError as json_decode_error:
        _logger.error('JSON decode error retrieving JSON content from URL:')
        _logger.exception(json_decode_error)
        raise
    except Exception as err:
        _logger.error('Error retrieving JSON content from URL :')
        _logger.exception(err)
        raise


def get_gmkf_studies(output_file_path: str, request_cookies: dict[str, str] = None) -> list[dict[str, any]]:
    """ get all gmkf studies available in API """
    if _CONFIG.get('USE_SAVED_SOURCE_DATA_FILE', True) and os.path.exists(output_file_path):
        _logger.info('Retrieving GMKF data from local source file "%s"', output_file_path)
        with open(output_file_path, 'r', encoding='utf-8') as fp:
            return json.load(fp)

    _logger.info('Loading GMKF studies and saving to "%s"', output_file_path)
    json_data: dict[str, any] = get_json_from_url(_CONFIG['GMKF_STUDY_URL'], cookies=request_cookies)
    if 'entry' not in json_data:
        raise RuntimeError('Expected attribute "entry" not found in JSON response data')
    fp: io.TextIOWrapper
    with open(output_file_path, 'w', encoding='utf-8') as fp:
        json.dump(json_data['entry'], fp)

    return json_data['entry']


def get_gmkf_subjects_all(output_file_path: str, request_cookies: dict[str, str] = None) -> list[dict[str, any]]:
    """ get all gmkf subjects available in API """
    if _CONFIG.get('USE_SAVED_SOURCE_DATA_FILE', True) and os.path.exists(output_file_path):
        _logger.info('Retrieving GMKF data from local source file "%s"', output_file_path)
        with open(output_file_path, 'r', encoding='utf-8') as fp:
            return json.load(fp)

    _logger.info('Loading all GMKF studies and saving to "%s"', output_file_path)

    json_data: dict[str, any] = get_json_from_url(_CONFIG['GMKF_SUBJECT_URL'], cookies=request_cookies)

    total_entries: int = json_data['total']
    entries_processed: int = 0
    subjects: list[dict[str, any]] = []
    while True:
        # enumerate entries returned by GMKF API and then follow 'link' if populated
        _logger.info(
            'Processing %d => %d of %d remote subject entries',
            entries_processed + 1,
            entries_processed + len(json_data['entry']),
            total_entries
        )
        entries_processed += len(json_data['entry'])
        subjects.extend(json_data['entry'])

		# 'link' property will be populated with a follow-up URL for paged results
        next_page_url: str = None
        link: dict[str, any]
        for link in [l for l in json_data['link'] if l['relation'] == 'next']:
            # paged response
            next_page_url = link['url']
            break

        if not next_page_url:
            break

        json_data = get_json_from_url(next_page_url, cookies=request_cookies)

    _logger.info('Saving %d subjects to "%s"', len(subjects), output_file_path)
    fp: io.TextIOWrapper
    with open(output_file_path, 'w', encoding='utf-8') as fp:
        json.dump(subjects, fp)

    return subjects


def get_gmkf_subjects_by_study_id(
    study_id: str,
    output_file_path: str,
    request_cookies: dict[str, str] = None
) -> dict[str, dict[str, any]]:
    """ get gmkf fhir resource(s) for specified study """
    if _CONFIG.get('USE_SAVED_SOURCE_DATA_FILE', True) and os.path.exists(output_file_path):
        _logger.info('Retrieving GMKF data from local source file "%s"', output_file_path)
        with open(output_file_path, 'r', encoding='utf-8') as fp:
            return json.load(fp)

    lowest_usi_external_participant_ids: dict[str, str] = {}
    subjects: dict[str, dict[str, any]] = {}
    subject_usi_external_participant_ids: dict[str, str] = {}

    params: dict[str, str] = {'study': study_id}
    # ex: https://fhir.kidsfirstdrc.org/ResearchSubject?study=sd-dypmehhf
    json_data: dict[str, any] = get_json_from_url(_CONFIG['GMKF_SUBJECT_URL'], cookies=request_cookies, params=params)

    external_participant_id: str
    usi: str

    total_entries: int = json_data['total']
    entries_processed: int = 0
    while True:
        # enumerate entries returned by GMKF API and then follow 'link' if populated
        _logger.info(
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
                _logger.warning('No external participant id (secondary identity) found for subject entry: %s', entry)
                continue

            usi = get_external_participant_id_usi(external_participant_id)
            if not usi:
                _logger.warning('No USI found for subject entry: %s', entry)
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

    _logger.info('%d subjects with unique USIs found for %d total remote subjects', len(subjects), total_entries)

    _logger.info('Saving %d GMKF subjects to "%s"', len(subjects), output_file_path)
    with open(output_file_path, mode='w', encoding='utf-8') as fp:
        json.dump(subjects, fp)

    return subjects


def get_gmkf_study_id_by_title(study_title: str, request_cookies: dict[str, str] = None) -> str:
    """ get id of gmkf study with specified title """
    err_msg: str

    params: dict[str, str] = {'title': study_title}
    try:
        # e.g. https://fhir.kidsfirstdrc.org/ResearchStudy?title=Discovering...
        json_data: dict[str, any] = get_json_from_url(_CONFIG['GMKF_STUDY_URL'], cookies=request_cookies, params=params)

        studies: list[dict[str, any]] = json_data.get('entry', [])
        if len(studies) != 1:
            err_msg = f'ERROR: {len(studies)} studies returned with title {study_title}'
            _logger.critical(err_msg)
            _logger.critical(json_data)
            raise RuntimeError(err_msg)

        if studies[0]['resource'] and studies[0]['resource']['id']:
            return studies[0]['resource']['id']

        err_msg = 'ERROR: Unable to get study id, check the response body:'
        _logger.critical(err_msg)
        _logger.critical(json_data)
        raise RuntimeError(err_msg)
    except requests.exceptions.HTTPError as http_error:
        _logger.error('HTTP error retrieving getting study id by title:')
        _logger.exception(http_error)
        raise
    except json.decoder.JSONDecodeError as json_decode_error:
        _logger.error('JSON decode error getting study id by title:')
        _logger.exception(json_decode_error)
        raise
    except Exception as err:
        _logger.error('Error getting study id by title:')
        _logger.exception(err)
        raise


def get_gmkf_subjects_from_file(file_path: str) -> dict[str, dict[str, any]]:
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


def build_external_resource_file(
    gmkf_subjects: dict[str, dict[str, any]],
    gen3_subjects: dict[str, dict[str, any]],
    output_file_path: str
) -> None:
    """ Create TSV file for load into Gen3 portal from specified GMKF and Gen3 subject records """
    _logger.info('Building external resource file')

    external_references: list[dict[str, any]] = []

    gen3_subjects_processed: int = 0
    gen3_subject_submitter_id: str
    gen3_subject: dict[str, any]
    for gen3_subject_submitter_id, gen3_subject in gen3_subjects.items():
        gen3_subjects_processed += 1
        if gen3_subjects_processed % 1000 == 0:
            _logger.info(
                '%d/%d subjects processed, processing submitter_id %s)',
                gen3_subjects_processed,
                len(gen3_subjects),
                gen3_subject_submitter_id
            )

        external_reference_submitter_id: str = f'external_reference_gmkf_{gen3_subject_submitter_id}_1'

        gmkf_submitter_id: str = gen3_subject['*honest_broker_subject_id']
        if not gmkf_subjects.get(gmkf_submitter_id):
            continue

        external_obj: dict[str, any] = {}
        external_obj['type'] = 'external_reference'
        external_obj['project_id'] = gen3_subject['project_id']
        external_obj['*subjects.submitter_id'] = gen3_subject_submitter_id
        external_obj['external_resource_icon_path'] = _CONFIG['EXTERNAL_RESOURCE_ICON_PATH']
        external_obj['external_resource_id'] = 2
        external_obj['external_resource_name'] = _CONFIG['EXTERNAL_RESOURCE_NAME']
        external_obj['*submitter_id'] = external_reference_submitter_id

        # determine whether our source data was retrieved from flat file or API
        if gmkf_subjects[gmkf_submitter_id].get('resource', {}).get('identifier'):
            # API-sourced subject record
            identifier: dict[str, any]
            for identifier in gmkf_subjects[gmkf_submitter_id]['resource']['identifier']:
                if (
                    identifier['use'] == 'official'
                    and
                    identifier.get('system') == _CONFIG['RESOURCE_ID_SYSTEM_PARTICIPANTS_URL']
                ):
                    external_obj['external_subject_submitter_id'] = str(identifier['value'])
                    external_obj['external_subject_url'] = (
                        _CONFIG['EXTERNAL_SUBJECT_URL_PREFIX'] + external_obj['external_subject_submitter_id']
                    )
                elif identifier.get('system') == _CONFIG['RESOURCE_ID_SYSTEM_UNIQUE_STRING_URN']:
                    external_obj['external_subject_id'] = str(identifier['value'])
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

    if not external_references:
        _logger.warning('No new/updated external references loaded, tsv output file not created')
        return

    _logger.info(
        '%d subjects processed, %d external references loaded, creating tsv output file',
        gen3_subjects_processed, len(external_references)
    )

    fp: io.TextIOWrapper
    with open(output_file_path, 'w', encoding='utf-8') as fp:
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
        writer: csv.DictWriter = csv.DictWriter(fp, fieldnames=fieldnames, dialect='excel-tab')
        writer.writeheader()
        writer.writerows(external_references)


def main():
    """ Standalone entry point """
    literal_eval_config_vars: dict[str, str] = {'USE_SAVED_SOURCE_DATA_FILE': 'False'}
    literal_eval_config_var_name: str
    literal_eval_config_var_default_val: str
    for literal_eval_config_var_name, literal_eval_config_var_default_val in literal_eval_config_vars.items():
        if literal_eval_config_var_name in _CONFIG:
            _CONFIG[literal_eval_config_var_name] = ast.literal_eval(
                str(_CONFIG.get(literal_eval_config_var_name, literal_eval_config_var_default_val))
            )

    gmkf_subjects: dict[str, dict[str, any]] = {}
    if _CONFIG.get('GMKF_SUBJECT_FILE_PATH'):
        _logger.info('Building external resource file using source file %s', _CONFIG['GMKF_SUBJECT_FILE_PATH'])
        gmkf_subjects = get_gmkf_subjects_from_file(_CONFIG['GMKF_SUBJECT_FILE_PATH'])
        if not gmkf_subjects:
            raise RuntimeError(
                f'No subjects found for subjects in "{_CONFIG["LOCAL_FILE_PATH"]}", external resource file not built'
            )
    else:
        _logger.info('Building external resource file using FHIR API')
        cookies: dict[str, str] = {'arc-user': _CONFIG['GMKF_AUTH_ARC_USER']}
        study_title: str
        for study_title in _CONFIG['GMKF_STUDY_TITLES']:
            study_id: str = get_gmkf_study_id_by_title(study_title, cookies)
            study_subjects: dict[str, dict[str, any]] = get_gmkf_subjects_by_study_id(
                study_id,
                f'./gmkf_data_{study_id}.json',
                cookies
            )
            _logger.info('%d subjects found for study "%s" ("%s")', len(study_subjects), study_id, study_title)
            gmkf_subjects = {**gmkf_subjects, **study_subjects}
        if not gmkf_subjects:
            raise RuntimeError(f'No GMKF subjects found for specified studies: "{_CONFIG["GMKF_STUDY_TITLES"]}"')

    _logger.info('%d GMKF subjects loaded', len(gmkf_subjects))

    output_file_name: str = _CONFIG.get('OUTPUT_FILE_NAME', 'gen3_external_reference_gmkf.tsv')
    gen3_subject_dir_paths: list[str] = json.loads(_CONFIG.get('GEN3_SUBJECT_DIR_PATHS', '[]'))
    gen3_subject_dir_ignore_paths: list[str] = json.loads(_CONFIG.get('GEN3_SUBJECT_DIR_IGNORE_PATHS', '[]'))

    gen3_subject_file_paths: list[str] = []
    gen3_subject_dir_path: str
    for gen3_subject_dir_path in gen3_subject_dir_paths:
        gen3_subject_file_paths.extend(get_all_subject_files(gen3_subject_dir_path, gen3_subject_dir_ignore_paths))

    if not gen3_subject_file_paths:
        raise RuntimeError('No subject files found; check source subject and ignore path(s) in config')

    _logger.info('Processing %d Gen3 TSV subject dir path(s)', len(gen3_subject_dir_paths))
    output_files_created: list[str] = []
    output_file_path: str
    subject_file_processing_index: int = 1
    gen3_subject_file_path: str
    for gen3_subject_file_path in gen3_subject_file_paths:
        gen3_subjects: dict[str, dict[str, any]] = get_gen3_subjects(gen3_subject_file_path)
        gen3_subjects = {k:v for k,v in gen3_subjects.items() if k.startswith('COG_')}
        if not gen3_subjects:
            _logger.info('No COG subjects found in "%s", skipping', gen3_subject_file_path)
            continue

        output_file_path = os.path.join(Path(gen3_subject_file_path).parent.absolute(), output_file_name)
        _logger.info(
            '%d/%d: Building Gen3 external reference TSV file for %d COG subjects in "%s" and saving to "%s"',
            subject_file_processing_index,
            len(gen3_subject_file_paths),
            len(gen3_subjects),
            gen3_subject_file_path,
            output_file_path
        )

        build_external_resource_file(gmkf_subjects, gen3_subjects, output_file_path)
        if not os.path.exists(output_file_path):
            _logger.warning('Output file "%s" not found, verify output file build was successful', output_file_path)
        else:
            output_files_created.append(output_file_path)
        subject_file_processing_index += 1

    _logger.info('%d external reference output file(s) created:', len(output_files_created))
    for output_file_path in output_files_created:
        _logger.info(output_file_path)
    #build_external_resource_file(_CONFIG['LOCAL_FILE_PATH'], subjects)


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
    'GMKF_STUDY_TITLES': [
        'Discovering the Genetic Basis of Human Neuroblastoma: A Gabriella Miller Kids First Pediatric Research ' +
            'Program (Kids First) Project'
    ],
    'EXTERNAL_RESOURCE_ICON_PATH': (
        'https://pcdc-external-resource-files.s3.us-east-1.amazonaws.com/' +
            'Kids_First_Graphic_Horizontal_OL_FINAL.DRC-01-scaled.png'
    ),
    'RESOURCE_ID_SYSTEM_PARTICIPANTS_URL': 'https://kf-api-dataservice.kidsfirstdrc.org//participants/',
    'RESOURCE_ID_SYSTEM_UNIQUE_STRING_URN': 'urn:kids-first:unique-string',
    'EXTERNAL_SUBJECT_URL_PREFIX': 'https://portal.kidsfirstdrc.org/participants/',
    'EXTERNAL_RESOURCE_NAME': 'GMKF',
    'USE_SAVED_SOURCE_DATA_FILE': True,
    'GEN3_SUBJECT_DIR_PATHS': '["/path/to/parent/or/root/dir/containing/gen3/subject/tsv/files/"]',
    'GEN3_SUBJECT_DIR_IGNORE_PATHS': '["/_"]',
    'OUTPUT_FILE_NAME': 'gen3_external_reference_gdc.tsv'
}
# override config defaults (or set FHIR API auth; see comments above) using .env config file in parent directory
_config_file_path: str = sys.argv[1] if len(sys.argv) == 2 else '.env_get_gmkf_data'
_config_file_vals: dict[str, str] = dotenv.dotenv_values(_config_file_path)
if not os.path.isfile(_config_file_path):
    raise FileNotFoundError(f'Config file "{_config_file_path}" not found')
_CONFIG.update(_config_file_vals)

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

_logger: logging.Logger = logging.getLogger()


if __name__ == '__main__':
    main()
