"""
Retrieve nationwide tissue bank sample data from D4CG AWS S3 json file created by AWS lambda (maintained by Paul/Luca)
"""
from collections.abc import Iterator
import csv
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
#   [profile nationwide-tissue-bank-staging]
#   role_arn=arn:aws:iam::<nationwide data manager role id number>:role/nationwide-data-manager
#   source_profile=portal-staging

_CONFIG: dict[str, any] = {
    'LOG_FILE_PATH': './get_nationwide_tissue_bank_data.log',
    'LOG_FILE_APPEND': False,
    'AWS_PROFILE_NAME': 'nationwide-tissue-bank-staging',
    'S3_BUCKET_NAME': 'nationwide-tissue-bank-data-staging',
    'S3_FILE_PREFIX': 'nationwide_data_',
    'USE_SAVED_SOURCE_DATA_FILE': True,
    'GEN3_SUBJECT_DIR_PATHS': [
        # Specify in local .env config file to avoid having to hard-code local parent path(s) holding
        # gen3_subject.tsv files containing subject submitter ids to be matched against subject ids
        # ('NCH_Assigned_Patient_USI' property). Directory paths will be recursively searched. An output file
        # (e.g. gen3_biospecimen_new.tsv; see 'OUTPUT_FILE_NAME' below) will be created in the same directory
        # for each gen3_subject.tsv found/specified.
    ],
    'GEN3_SUBJECT_DIR_IGNORE_PATHS': [
        # Specify full or partial path by which to specify files/directories to be ignored when searching for
        # gen3_subject.tsv files. Implemented using 'contains' (e.g. 'needle' in 'haystack') logic.
    ],
    'OUTPUT_FILE_NAME': 'gen3_biospecimen_new.tsv'
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


def download_latest_data_file_from_s3(
    aws_profile_name: str,
    s3_bucket_name: str,
    local_save_path: str,
    data_file_prefix: str = 'nationwide_data_'
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
            usi: str = record['*honest_broker_subject_id'].strip().upper()
            if usi in subjects:
                _logger.warning('Subject USI "%s" loaded more than once')
            subjects[usi] = record
    _logger.info('Loaded %d Gen3 subject records', len(subjects))
    return subjects


def get_biospecimen_source_data(source_file_path: str) -> list[dict[str, any]]:
    """ Load and return biospecimen records from specified file path """
    biospecimen_source_data: list[dict[str, any]] = []
    fd_data: typing.TextIO
    _logger.info('Loading biospecimen data from source file "%s""', source_file_path)
    if not os.path.isfile(source_file_path):
        raise RuntimeError(f'Source file "{source_file_path}" not found')

    with open(source_file_path, encoding='utf-8') as fd_data:
        biospecimen_source_data = json.load(fd_data)
    if not biospecimen_source_data:
        raise RuntimeError(f'No records found in biospecimen source file "{source_file_path}"')

    # sort source records for consistent (idempotent) output for data-equivalent source files
    _logger.info('%d source records loaded, sorting', len(biospecimen_source_data))
    biospecimen_source_data.sort(
        key=lambda r: (
            r['NCH_Assigned_Patient_USI'],
            r.get('Protocol_Codes', '') or '',
            r.get('Biospecimen_Type_Summary', '') or '',
            r.get('Current_Status', '') or '',
            r.get('Biospecimen_Media', '') or '',
            r.get('Collection_Timepoint', '') or '',
            r.get('Qty_Current', '') or '',
            r.get('Qty_Current_Value', 0) or 0,
            r.get('Qty_Current_UoM', '') or '',
            r.get('Biospecimen_Unit_Type', '') or '',
        )
    )
    return biospecimen_source_data


def get_biospecimen_source_data_indexed(source_file_path: str) -> dict[str, list[dict[str, any]]]:
    """ Load and return biospecimen records from specified file path indexed by subject usi """
    biospecimen_source_data: list[dict[str, any]] = get_biospecimen_source_data(source_file_path)
    _logger.info('Indexing biospecimen source data')
    if any(not s.get('NCH_Assigned_Patient_USI') for s in biospecimen_source_data):
        raise RuntimeError('"NCH_Assigned_Patient_USI" blank/null for one or more records in biospecimen source data')

    biospecimen_source_data_indexed: dict[str, list[dict[str, any]]] = {}
    biospecimen_source_record: dict[str, any]
    for biospecimen_source_record in biospecimen_source_data:
        subject_usi: str = biospecimen_source_record['NCH_Assigned_Patient_USI']
        biospecimen_source_data_indexed[subject_usi] = biospecimen_source_data_indexed.get(subject_usi, [])
        biospecimen_source_data_indexed[subject_usi].append(biospecimen_source_record)
    return biospecimen_source_data_indexed


def build_gen3_biospecimen_record(
    subject_submitter_id: str,
    biospecimen_source_record: dict[str, any],
    project_id: str,
    subject_submitter_id_counts: dict[str, int]
) -> dict[str, any]:
    """ Create and return biospecimen record from specified source record and subject submitter id counts """
    qty_val: str = biospecimen_source_record.get('Qty_Current_Value', '')
    if qty_val not in ('', None) and is_number(qty_val):
        qty_val_num: float = float(qty_val)
        if qty_val_num.is_integer():
            qty_val = str(int(qty_val_num))
        else:
            qty_val = round(qty_val_num, 2)
    else:
        qty_val = ''

    subject_submitter_id_count: int = subject_submitter_id_counts.get(subject_submitter_id)
    if not subject_submitter_id_count:
        raise RuntimeError(f'Subject submitter id count not found for subject "{subject_submitter_id}"')
    output_submitter_id: str = f'biospecimen_{subject_submitter_id}_{subject_submitter_id_count}'
    # set sort key e.g. 'biospecimen_COG_PABCDEF_1' => 'biospecimen_COG_PABCDEF_00001' for natural sort ordering
    sortkey: str = f'biospecimen_{subject_submitter_id}_{subject_submitter_id_count:05}'
    return {
        'sortkey': sortkey,
        'type': 'biospecimen',
        'project_id': project_id,
        '*submitter_id': output_submitter_id,
        '*subjects.submitter_id': subject_submitter_id,
        'biospecimen_container_type': biospecimen_source_record.get('Biospecimen_Unit_Type', ''),
        'biospecimen_media': biospecimen_source_record.get('Biospecimen_Media', ''),
        'biospecimen_type': biospecimen_source_record.get('Biospecimen_Type_Summary', ''),
        'current_qty_value': qty_val,
        'current_qty_unit': biospecimen_source_record.get('Qty_Current_UoM', '')
    }


def build_gen3_biospecimen_file(
    biospecimen_records: dict[str, list[dict[str, any]]],
    gen3_subjects: dict[str, dict[str , any]],
    output_file_path: str
) -> None:
    """ Create TSV file for load into Gen3 portal from specified biospecimen and Gen3 subject records """
    _logger.info('Building biospecimen output file')

    project_ids: set[str] = {v['project_id'] for v in gen3_subjects.values()}
    if len(project_ids) != 1:
        raise RuntimeError(f'Number of subject project_id values != 1: {project_ids}')

    project_id: str = project_ids.pop()
    subject_submitter_id_counts: dict[str, int] = {v['*submitter_id']:1 for v in gen3_subjects.values()}
    subjects_found: set[str] = set()
    subjects_not_found: set[str] = set()

    num_subjects: int = len(gen3_subjects)
    num_subjects_processed: int = 0

    num_depleted_records: int = 0

    output_records: list[dict[str, any]] = []

    subject_usi: str
    subject_record: dict[str, any]
    for subject_usi, subject_record in gen3_subjects.items():
        num_subjects_processed += 1
        if num_subjects_processed % 1000 == 0:
            _logger.info(
                '%d of %d subjects processed, %d output records created for %d subjects',
                num_subjects_processed,
                num_subjects,
                len(output_records),
                len(subjects_found)
            )

        gen3_subject_id: str = subject_record['*submitter_id']

        # find source records
        subject_biospecimen_records: list[dict[str, any]] = biospecimen_records.get(subject_usi, [])
        if not subject_biospecimen_records:
            # _logger.warning(
            #     'No source biospecimen data found for Gen3 subject "%s", biospecimen record(s) not populated',
            #     gen3_subject_id
            # )
            subjects_not_found.add(gen3_subject_id)
            continue
        subjects_found.add(gen3_subject_id)

        subject_biospecimen_record: dict[str, any]
        for subject_biospecimen_record in subject_biospecimen_records:
            # verify that source record is not depleted
            if subject_biospecimen_record.get('Current_Status', '').upper() == 'DEPLETED':
                num_depleted_records += 1
                continue

            output_records.append(
                build_gen3_biospecimen_record(
                    gen3_subject_id,
                    subject_biospecimen_record,
                    project_id,
                    subject_submitter_id_counts
                )
            )
            subject_submitter_id_counts[gen3_subject_id] += 1

    if num_subjects_processed % 1000 != 0:
        _logger.info(
            '%d of %d subjects processed, %d output records created for %d subjects',
            num_subjects_processed,
            num_subjects,
            len(output_records),
            len(subjects_found)
        )
    if not output_records:
        _logger.warning("No biospecimen output records to write")
        return

    # sort records and remove sort key from final output records
    output_records.sort(key=lambda r: r['sortkey'])
    for output_record in output_records:
        output_record.pop('sortkey')

    # save biospecimen records to specified output path
    fd_tsv: typing.TextIO
    with open(output_file_path, mode='w', encoding='utf-8') as fd_tsv:
        writer: csv.DictWriter = csv.DictWriter(fd_tsv, fieldnames=output_records[0].keys(), delimiter='\t')
        writer.writeheader()
        output_record: dict[str, any]
        for output_record in output_records:
            writer.writerow(output_record)
    _logger.info('Saved %d output records to "%s"', len(output_records), output_file_path)
    _logger.info(
        '%d distinct subjects processed, %d subjects found in biospecimen source data, %d not found',
        len(subjects_found) + len(subjects_not_found),
        len(subjects_found),
        len(subjects_not_found)
    )
    if num_depleted_records:
        _logger.info('%d source record(s) matched subjects but excluded due to "DEPLETED" status')


def main():
    """
    Standalone entry point
    """
    local_data_file_path: str = './nationwide_tissue_bank_data.json'
    if not _CONFIG.get('USE_SAVED_SOURCE_DATA_FILE', True) or not os.path.exists(local_data_file_path):
        local_data_file_path_last: str = './nationwide_tissue_bank_data_last.json'
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

    biospecimen_data: dict[str, list[dict[str, any]]] = get_biospecimen_source_data_indexed(local_data_file_path)

    output_file_name: str = _CONFIG.get('OUTPUT_FILE_NAME', 'gen3_biospecimen_new.tsv')
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
            '%d/%d: Building Gen3 biospecimen TSV file for %d subjects in "%s" and saving to "%s"',
            subject_file_processing_index,
            len(gen3_subject_file_paths),
            len(gen3_subjects),
            gen3_subject_file_path,
            output_file_path
        )
        build_gen3_biospecimen_file(biospecimen_data, gen3_subjects, output_file_path)
        if not os.path.exists(output_file_path):
            _logger.warning('Output file "%s" not found, verify output file build was successful', output_file_path)
        else:
            output_files_created.append(output_file_path)
        subject_file_processing_index += 1

    _logger.info('%d biospecimen output file(s) created:', len(output_files_created))
    for output_file_path in output_files_created:
        _logger.info(output_file_path)


if __name__ == '__main__':
    main()
