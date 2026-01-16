"""
Retrieve external reference data from GDC
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


def get_gdc_subject_usi(subject: dict[str, any]) -> str:
    """
    get USI for specified GDC subject having submitter id in 'TARGET-##-{USI}' format e.g. 'TARGET-##-ABCDEF' => ABCDEF
    """
    submitter_id: str = subject.get('submitter_id', '')
    if not submitter_id or not submitter_id.startswith('TARGET-'):
        raise RuntimeError(f'Subject submitter id missing or invalid: "{subject}"')
    submitter_id = submitter_id[len('TARGET-'):]

    return None if '-' not in submitter_id else submitter_id.partition('-')[-1]

def get_gdc_subject_project_id(subject: dict[str, any]) -> str:
    """
    get project id for specified GDC subject having nested project e.g. 'project': {'project_id': 'TARGET-NBL'}
    """
    return subject.get('project', {}).get('project_id')


def get_gdc_target_data(output_file_path: str) -> dict[str, list[dict[str, any]]]:
    """ Retrieve GDC TARGET records as list of subject records per USI """
    if _CONFIG.get('USE_SAVED_SOURCE_DATA_FILE', True) and os.path.exists(output_file_path):
        _logger.info('Retrieving GDC TARGET data from local source file "%s"', output_file_path)
        fp: io.TextIOWrapper
        with open(output_file_path, 'r', encoding='utf-8') as fp:
            return json.load(fp)

    _logger.info('Retrieving GDC TARGET data from "%s"', _CONFIG['GDC_API_ENDPOINT'])
    output_file_path_last: str = './gdc_target_data_last.json'
    if os.path.exists(output_file_path):
        if os.path.exists(output_file_path_last):
            os.remove(output_file_path_last)
        os.rename(output_file_path, output_file_path_last)
        if os.path.exists(output_file_path):
            raise RuntimeError(f'Unable to rename "{output_file_path}" to "{output_file_path_last}"')

    filters: dict[str, any] = {
        'op': 'and',
        'content': [
            {
                'op': '=',
                'content': {
                    'field': 'submitter_id',
                    'value': 'TARGET-*'
                }
            },
            {
                'op': '=',
                'content': {
                    'field': 'project.project_id',
                    'value': 'TARGET-*'
                }
            }
        ]
    }

    # sort fields specified for repeatable ordering of external reference records for subjects with multiple records
    # note spaces not allowed in "fields" and "sort" params
    params: dict[str, str] = {
        'filters': json.dumps(filters),
        'fields': 'submitter_id,project.project_id,created_datetime',
        'format': 'JSON',
        'sort': 'submitter_id,created_datetime:asc,updated_datetime:asc,project.project_id',
        'size': 1000,
        'from': 0
    }

    subjects: dict[str, list[dict[str, any]]] = {}
    subject: dict[str, any]
    while True:
        response: requests.Response = requests.get(_CONFIG['GDC_API_ENDPOINT'], params=params, timeout=30)
        pagination: dict[str, any] = json.loads(response.content)['data']['pagination']
        params['from'] += pagination['size']
        if int(pagination['count']) == 0 or int(pagination['size']) == 0:
            break
        _logger.info(
            'Loading %d GDC subjects (%d => %d), %d total',
            pagination['count'],
            pagination['from'],
            pagination['from'] + pagination['size'],
            pagination['total'],
        )
        page_subjects: list[dict[str, any]] = json.loads(response.content)['data']['hits']
        for subject in page_subjects:
            usi: str = get_gdc_subject_usi(subject)
            project_id: str = get_gdc_subject_project_id(subject)
            if not usi or not project_id:
                raise RuntimeError(f'Missing submitter id or project id for GDC subject: {subject}')

            subjects[usi] = subjects.get(usi, [])
            subjects[usi].append(subject)

    if not subjects:
        raise RuntimeError('No GDC subjects found')

    multi_rec_usis: list[str] = [k for k,v in subjects.items() if len(v) > 1]
    if multi_rec_usis:
        _logger.warning('%d GDC subject USIs with multiple records: %s', len(multi_rec_usis), multi_rec_usis)

    _logger.info('Saving %d GDC subjects to "%s"', len(subjects), output_file_path)
    with open(output_file_path, mode='w', encoding='utf-8') as fp:
        json.dump(subjects, fp)

    return subjects


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


def build_external_resource_file(
    gdc_usi_subjects: dict[str, list[dict[str, any]]],
    gen3_subjects: dict[str, dict[str, any]],
    output_file_path: str
) -> None:
    """ Create TSV file for load into Gen3 portal from specified GDC TARGET and Gen3 subject records """
    _logger.info('Building external resource file')

    external_references: list[dict[str, any]] = []

    gen3_subjects_processed: int = 0
    gen3_subject_submitter_id: str
    gen3_subject: dict[str, any]
    for gen3_subject_submitter_id, gen3_subject in gen3_subjects.items():
        gen3_subjects_processed += 1
        if gen3_subjects_processed % 1000 == 0:
            _logger.info(
                '%d/%d subjects processed, processing submitter_id "%s")',
                gen3_subjects_processed,
                len(gen3_subjects),
                gen3_subject_submitter_id
            )

        # ex: COG_PACLAX => data contributor = COG, USI = PACLAX
        gen3_subject_submitter_id_parts: list[str] =  gen3_subject_submitter_id.split('_')
        if len(gen3_subject_submitter_id_parts) < 2:
            _logger.warning('Unexpected/malformed submitter_id: "%s"', gen3_subject_submitter_id)
            continue

        usi: str = gen3_subject['*honest_broker_subject_id'].strip().upper()
        if usi not in gdc_usi_subjects:
            continue

        external_reference_index: int
        gdc_subject: dict[str, any]
        for external_reference_index, gdc_subject in enumerate(gdc_usi_subjects[usi], 1):
            external_reference_submitter_id: str = (
                f"external_reference_gdc_{gen3_subject_submitter_id}_{external_reference_index}"
            )

            external_obj: dict[str, any] = {}
            external_obj['type'] = 'external_reference'
            external_obj['project_id'] = gen3_subject['project_id']
            external_obj['*subjects.submitter_id'] = gen3_subject_submitter_id
            external_obj['external_resource_icon_path'] = _CONFIG['EXTERNAL_RESOURCE_ICON_PATH']
            external_obj['external_resource_id'] = 1
            external_obj['external_resource_name'] = _CONFIG['EXTERNAL_RESOURCE_NAME']
            external_obj['*submitter_id'] = external_reference_submitter_id

            external_obj['external_subject_url'] = _CONFIG['EXTERNAL_SUBJECT_URL_PREFIX'] + str(gdc_subject['id'])
            external_obj['external_subject_id'] = str(gdc_subject['id'])
            external_obj['external_subject_submitter_id'] = str(gdc_subject['submitter_id'])
            external_obj['external_links'] = (
                external_obj['external_resource_name'] + '|' +
                external_obj['external_resource_icon_path'] + '|' +
                external_obj['external_subject_url']
            )

            external_references.append(external_obj)

    if not external_references:
        _logger.warning('No external references loaded, output file not created')
        return

    _logger.info(
        '%d subjects processed, %d external references loaded, creating tsv output file',
        gen3_subjects_processed, len(external_references)
    )

    fp: io.TextIOWrapper
    with open(output_file_path, mode='w', encoding='utf-8') as fp:
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

    local_data_file_path: str = './gdc_target_data.json'
    gdc_usi_subjects: dict[str, list[dict[str, any]]] = get_gdc_target_data(local_data_file_path)

    if not gdc_usi_subjects:
        raise RuntimeError('No GDC TARGET subjects found')

    _logger.info('%d GDC TARGET subjects loaded', len(gdc_usi_subjects))

    output_file_name: str = _CONFIG.get('OUTPUT_FILE_NAME', 'gen3_external_reference_gdc.tsv')
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

        build_external_resource_file(gdc_usi_subjects, gen3_subjects, output_file_path)
        if not os.path.exists(output_file_path):
            _logger.warning('Output file "%s" not found, verify output file build was successful', output_file_path)
        else:
            output_files_created.append(output_file_path)
        subject_file_processing_index += 1

    _logger.info('%d external reference output file(s) created:', len(output_files_created))
    for output_file_path in output_files_created:
        _logger.info(output_file_path)


_CONFIG: dict[str, any] = {
    'LOG_FILE_PATH': './get_target_data.log',
    'LOG_FILE_APPEND': False,
    'GDC_API_ENDPOINT': 'https://api.gdc.cancer.gov/cases',
    'EXTERNAL_RESOURCE_ICON_PATH': (
        'https://pcdc-external-resource-files.s3.amazonaws.com/NHI_GDC_DataPortal-logo.23e6ca47.svg'
    ),
    'EXTERNAL_SUBJECT_URL_PREFIX': 'https://portal.gdc.cancer.gov/cases/',
    'EXTERNAL_RESOURCE_NAME': 'TARGET - GDC',
    'USE_SAVED_SOURCE_DATA_FILE': True,
    'GEN3_SUBJECT_DIR_PATHS': '["/path/to/parent/or/root/dir/containing/gen3/subject/tsv/files/"]',
    'GEN3_SUBJECT_DIR_IGNORE_PATHS': '["/_"]',
    'OUTPUT_FILE_NAME': 'gen3_external_reference_gdc.tsv'
}
# run command: python script.py .env
_config_file_path: str = sys.argv[1] if len(sys.argv) == 2 else '.env_get_target_data'
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
