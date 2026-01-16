"""
Retrieve imaging data commons data from D4CG AWS S3 json file created by AWS lambda (maintained by Luca)
"""
from collections.abc import Iterator
import csv
import io
import json
import logging
import os
from pathlib import Path
import sys
import typing
from urllib.parse import ParseResult, urlparse, urlunparse

import boto3
import dotenv
from botocore.exceptions import ClientError


# suppress DEBUG logging from s3 transfers
logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)
logging.getLogger('s3transfer').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

# AWS credentials and config needed in addition to config vars below:
# ~/.aws/credentials:
#   [portal-staging]
#   aws_access_key_id = ACCESS_KEY_ID
#   aws_secret_access_key = SECRET_ACCESS_KEY

# ~/.aws/config:
#   # default profile must have permission to download source data from S3 bucket
#   [default]
#   region = us-east-1
_CONFIG: dict[str, any] = {
    'LOG_FILE_PATH': './get_idc_data.log',
    'LOG_FILE_APPEND': False,
    'AWS_PROFILE_NAME': 'idc-staging',
    'S3_BUCKET_NAME': 'idc-index-pull-dev-idc-index-bucket',
    'S3_FILE_PREFIX': 'idc_index_',
    'EXTERNAL_RESOURCE_ICON_PATH': (
        'https://storage.googleapis.com/idc-prod-web-static-files/static/img/NIH_IDC_title.svg'
    ),
    'EXTERNAL_RESOURCE_NAME': 'Imaging Data Commons',
    'USE_SAVED_SOURCE_DATA_FILE': True,
    'GEN3_SUBJECT_DIR_PATHS': [
        # Specify in local .env config file to avoid having to hard-code local parent path(s) holding
        # gen3_subject.tsv files containing subject submitter ids to be matched against subject ids
        # ('PatientID' property). Directory paths will be recursively searched. An output file
        # (e.g. gen3_biospecimen_new.tsv; see 'OUTPUT_FILE_NAME' below) will be created in the same
        # directory for each gen3_subject.tsv found/specified.
    ],
    'GEN3_SUBJECT_DIR_IGNORE_PATHS': [
        # Specify full or partial path by which to specify files/directories to be ignored when searching for
        # gen3_subject.tsv files. Implemented using 'contains' (e.g. 'needle' in 'haystack') logic.
    ],
    'OUTPUT_FILE_NAME': 'gen3_external_reference_idc.tsv'
}
# run command: python script.py .env
_config_file_path: str = sys.argv[1] if len(sys.argv) == 2 else '.env'
if not os.path.isfile(_config_file_path):
    raise FileNotFoundError(f'Config file "{_config_file_path}" not found')
_CONFIG.update(dotenv.dotenv_values(_config_file_path))

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


class AwsS3:
    """ Facilitate AWS S3 access """
    def __init__(self, profile_name: str = None) -> None:
        self._s3: any = None
        if profile_name:
            session: any = boto3.Session(profile_name=profile_name)
            self._s3 = session.client('s3')
        else:
            self._s3 = boto3.client('s3')

    @staticmethod
    def is_s3_uri(s3_uri: str) -> bool:
        """ Check if specified string is S3 URI """
        return str(s3_uri if s3_uri is not None else '').lower().startswith('s3://')

    @staticmethod
    def parse_s3_uri(s3_uri: str) -> tuple[str, str]:
        """ Parse specified S3 URI and return bucket name and object path as tuple """
        parse_result: ParseResult = urlparse(s3_uri, allow_fragments=False)
        return (parse_result.netloc, parse_result.path.lstrip('/'))

    @staticmethod
    def compose_s3_uri(bucket_name: str, object_path: str) -> str:
        """ Compose S3 URI from specified bucket name and object path """
        parse_result: ParseResult = ParseResult(
            scheme='S3',
            netloc=bucket_name,
            path=object_path,
            params='',
            query='',
            fragment=''
        )
        return urlunparse(parse_result)

    def bucket_exists(self, bucket_name: str) -> bool:
        """ Check if specified bucket exists and can be accessed by authenticated user """
        try:
            response: any = self._s3.head_bucket(Bucket=bucket_name)
            return bool(response)
        except ClientError:
            return False

    def get_buckets(self) -> list[any]:
        """ Get all S3 buckets owned by authenticated user """
        response: any = self._s3.list_buckets()
        return response.get('Buckets', [])

    def get_file_object_paths(self, bucket_name: str, prefix: str = '') -> Iterator[str]:
        """ Get list of all objects in specified S3 bucket with optional prefix """
        paginator: any = self._s3.get_paginator('list_objects_v2')
        pages: any = paginator.paginate(Bucket=bucket_name, Prefix=prefix)
        page: dict[str, any]
        for page in pages:
            content: dict[str, any]
            for content in page.get('Contents', {}):
                if 'Key' not in content:
                    raise RuntimeError(f'"Key" not found in page content item: {content}')
                yield content['Key']

    def get_file_metadata(self, bucket_name: str, object_path: str) -> any:
        """ Get metadata for specified S3 object """
        try:
            return self._s3.head_object(Bucket=bucket_name, Key=object_path)
        except ClientError:
            return None

    def get_file_size(self, bucket_name: str, object_path: str) -> int:
        """ Get size of specified S3 object """
        metadata: dict[str, any] = self.get_file_metadata(bucket_name, object_path)
        if not metadata:
            raise RuntimeError(f'File "{AwsS3.compose_s3_uri(bucket_name, object_path)}" not found')
        file_size: int = metadata.get('ContentLength', -1)
        if file_size < 0:
            raise RuntimeError(
                f'"ContentLength" attribute not found for file "{AwsS3.compose_s3_uri(bucket_name, object_path)}"'
            )
        return file_size

    def get_file_content(self, bucket_name: str, object_path: str) -> bytes:
        """ Get contents (bytes) of specified S3 bucket object """
        try:
            s3_object: any = self._s3.get_object(Bucket=bucket_name, Key=object_path)
            return s3_object['Body'].read() if s3_object else None
        except ClientError as err:
            _logger.error('Error getting content for object "%s" in bucket "%s": %s', object_path, bucket_name, err)
            return None

    def file_exists(self, bucket_name: str, object_path: str) -> bool:
        """ Check if specified S3 object exists in bucket by attempting to get file metadata """
        return bool(self.get_file_metadata(bucket_name, object_path))

    def upload_file(self, local_file_path: str, bucket_name: str, object_path: str = None) -> None:
        """ Upload specified file to bucket with S3 object name if provided, else file name """
        object_path = object_path if object_path else os.path.basename(local_file_path)
        try:
            self._s3.upload_file(Filename=local_file_path, Bucket=bucket_name, Key=object_path)
        except ClientError as err:
            _logger.error('Error uploading file "%s" to bucket "%s": %s', local_file_path, bucket_name, err)

    def download_file(self, bucket_name: str, object_path: str, local_file_path: str = None) -> None:
        """ Download specified S3 object to local file """
        local_file_path = local_file_path if local_file_path else object_path
        try:
            self._s3.download_file(Bucket=bucket_name, Key=object_path, Filename=local_file_path)
        except ClientError as err:
            _logger.error(
                'Error downloading object "%s" from bucket "%s" to local file "%s": %s',
                object_path,
                bucket_name,
                local_file_path,
                err
            )

    def delete_file(self, bucket_name: str, object_path: str) -> None:
        """ Delete specified S3 object from bucket """
        self._s3.delete_object(Bucket=bucket_name, Key=object_path)


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
            subjects[record['*submitter_id'].strip().upper()] = record
    _logger.info('Loaded %d Gen3 subject records', len(subjects))
    return subjects


def download_latest_data_file_from_s3(
    aws_profile_name: str,
    s3_bucket_name: str,
    local_save_path: str,
    data_file_prefix: str = 'idc_index_'
) -> None:
    """ Download most recent data file from S3 and save locally """
    _logger.info('Downloading latest data file from S3 bucket "%s" to "%s"', s3_bucket_name, local_save_path)
    aws_s3: AwsS3 = AwsS3(aws_profile_name)
    data_file_names: list[str] = list(aws_s3.get_file_object_paths(s3_bucket_name, data_file_prefix))
    if not data_file_names:
        err_msg: str = f'No data files found in bucket "{s3_bucket_name}"'
        err_msg += f' with prefix "{data_file_prefix}"' if data_file_prefix else ''
        raise RuntimeError(err_msg)
    data_file_names.sort(reverse=True)
    aws_s3.download_file(s3_bucket_name, data_file_names[0], local_save_path)
    _logger.info('Downloaded latest data file "%s"', data_file_names[0])
    fd_data: typing.TextIO
    data: list[dict[str, any]] = []
    _logger.info('Loading data file into memory to reformat')
    with open(local_save_path, encoding='utf-8') as fd_data:
        data = json.load(fd_data)
    _logger.info('Re-saving formatted data')
    with open(local_save_path, encoding='utf-8', mode='w') as fd_data:
        json.dump(data, fd_data, indent=2)
    os.rename(local_save_path, local_save_path.replace('.json', '_all.json'))

    # filter for records of interest
    _logger.info('Filtering for records of interest and sorting')
    idc_collection_ids: list[str] = json.loads(_CONFIG.get('IDC_COLLECTION_IDS', '[]'))
    data_filtered: list[dict[str, any]] = [
        d for d in data if not idc_collection_ids or d['collection_id'] in idc_collection_ids
    ]
    data_filtered.sort(
        key=lambda r: (
            r['PatientID'],
            r.get('collection_id', '') or '',
            r.get('StudyDate', '') or '',
            r.get('SeriesDate', '') or '',
            r.get('SeriesNumber', '') or '',
            r.get('series_aws_url', '') or ''
        )
    )
    with open(local_save_path, encoding='utf-8', mode='w') as fd_data:
        json.dump(data_filtered, fd_data, indent=2)


def get_idc_data(source_file_path: str) -> list[dict[str, any]]:
    """ Load and return IDC patient ids from specified file path """
    idc_source_data: list[dict[str, any]] = []
    fd_data: typing.TextIO
    _logger.info('Loading IDC data from source file "%s""', source_file_path)
    if not os.path.isfile(source_file_path):
        raise RuntimeError(f'Source file "{source_file_path}" not found')

    with open(source_file_path, encoding='utf-8') as fd_data:
        idc_source_data = json.load(fd_data)
    if not idc_source_data:
        raise RuntimeError(f'No records found in IDC source file "{source_file_path}"')

    idc_data_indexed: dict[str, list[dict[str, any]]] = {}
    idc_record: dict[str, any]
    for idc_record in idc_source_data:
        usi: str = idc_record['PatientID'].strip().upper()
        idc_data_indexed[usi] = idc_data_indexed.get(usi, [])
        idc_data_indexed[usi].append(idc_record)
    _logger.info(
        '%d total records loaded for %d unique subjects',
        sum(len(v) for v in idc_data_indexed.values()),
        len(idc_data_indexed)
    )
    return idc_data_indexed


def build_external_resource_file(
    idc_data: list[dict[str, any]],
    gen3_subjects: dict[str, dict[str, any]],
    output_file_path: str
) -> None:
    """ Create TSV file for load into Gen3 portal from specified IDC patient and Gen3 subject records """
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
        if usi not in idc_data:
            continue

        external_reference_index: int
        idc_record: dict[str, any]
        for external_reference_index, idc_record in enumerate(idc_data[usi], 1):
            external_reference_submitter_id: str = (
                f"external_reference_idc_{gen3_subject_submitter_id}_{external_reference_index}"
            )

            external_obj: dict[str, any] = {}
            external_obj['type'] = 'external_reference'
            external_obj['project_id'] = gen3_subject['project_id']
            external_obj['*submitter_id'] = external_reference_submitter_id
            external_obj['*subjects.submitter_id'] = gen3_subject_submitter_id
            external_obj['external_resource_icon_path'] = _CONFIG['EXTERNAL_RESOURCE_ICON_PATH']
            external_obj['external_resource_id'] = 3
            external_obj['external_resource_name'] = _CONFIG['EXTERNAL_RESOURCE_NAME']
            external_obj['external_subject_url'] = idc_record['series_aws_url']
            external_obj['external_subject_id'] = usi
            external_obj['external_subject_submitter_id'] = usi
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
    """
    Standalone entry point
    """
    local_data_file_path: str = './idc_index_data.json'
    if not _CONFIG.get('USE_SAVED_SOURCE_DATA_FILE', True) or not os.path.exists(local_data_file_path):
        local_data_file_path_last: str = './idc_index_data_last.json'
        if os.path.exists(local_data_file_path):
            if os.path.exists(local_data_file_path_last):
                os.remove(local_data_file_path_last)
            os.rename(local_data_file_path, local_data_file_path_last)
            if os.path.exists(local_data_file_path):
                raise RuntimeError(f'Unable to rename "{local_data_file_path}" to "{local_data_file_path_last}"')
        download_latest_data_file_from_s3(
            _CONFIG.get('AWS_PROFILE_NAME'),
            _CONFIG.get('S3_BUCKET_NAME'),
            local_data_file_path,
            _CONFIG.get('S3_FILE_PREFIX')
        )

    if not os.path.exists(local_data_file_path):
        raise RuntimeError(f'Source data file "{local_data_file_path}" not found, verify that download was successful')

    idc_data: dict[str, list[dict[str, any]]] = get_idc_data(local_data_file_path)
    if not idc_data:
        raise RuntimeError('No IDC data found')

    output_file_name: str = _CONFIG.get('OUTPUT_FILE_NAME', 'gen3_external_reference_idc.tsv')
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
        output_file_path = os.path.join(Path(gen3_subject_file_path).parent.absolute(), output_file_name)
        _logger.info(
            '%d/%d: Building Gen3 external reference TSV file for %d subjects in "%s" and saving to "%s"',
            subject_file_processing_index,
            len(gen3_subject_file_paths),
            len(gen3_subjects),
            gen3_subject_file_path,
            output_file_path
        )

        build_external_resource_file(idc_data, gen3_subjects, output_file_path)
        if not os.path.exists(output_file_path):
            _logger.warning('Output file "%s" not found, verify output file build was successful', output_file_path)
        else:
            output_files_created.append(output_file_path)
        subject_file_processing_index += 1

    _logger.info('%d external reference output file(s) created:', len(output_files_created))
    for output_file_path in output_files_created:
        _logger.info(output_file_path)


if __name__ == '__main__':
    main()
