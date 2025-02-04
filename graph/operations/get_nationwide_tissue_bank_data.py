"""
Retrieve nationwide tissue bank sample data from D4CG AWS S3 json file created by AWS lambda (maintained by Paul/Luca)
"""
import csv
import json
import logging
import os
import sys
import typing
from collections.abc import Iterator
from urllib.parse import ParseResult, urlparse, urlunparse

import boto3
import dotenv
from botocore.exceptions import ClientError

# suppress DEBUG logging from s3 transfers
logging.getLogger('boto3').setLevel(logging.ERROR)
logging.getLogger('botocore').setLevel(logging.ERROR)
logging.getLogger('s3transfer').setLevel(logging.ERROR)
logging.getLogger('urllib3').setLevel(logging.ERROR)

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


def is_number(value: str):
    """ Determine whether specified string is number (float or int) """
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


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


def get_gen3_subjects(gen3_subject_tsv_file_paths: list[str]) -> list[dict[str, any]]:
    """ Load and return collection of Gen3 subject records from specified file paths (gen3_subject.tsv) """
    _logger.info('Loading gen3 subjects')
    fd_subjects: typing.TextIO
    gen3_subject_tsv_file_path: str
    subjects: dict[str, dict[str, any]] = {}
    for gen3_subject_tsv_file_path in gen3_subject_tsv_file_paths:
        _logger.info('Loading gen3 subjects from "%s"', gen3_subject_tsv_file_path)
        with open(gen3_subject_tsv_file_path, 'r', encoding='utf-8') as fd_subjects:
            reader: csv.DictReader = csv.DictReader(fd_subjects, delimiter='\t')
            record: dict[str, any]
            for record in reader:
                if record['*submitter_id'] in subjects:
                    _logger.warning('Subject "%s" loaded more than once')
                subjects[record['*submitter_id']] = record
    _logger.info('Loaded %d Gen3 subject records', len(subjects))
    return subjects


def build_biospecimen_record(
    source_record: dict[str, any],
    project_id: str,
    subject_submitter_id_counts: dict[str, int]
) -> dict[str, any]:
    """ Create and return biospecimen record from specified source record and subject submitter id counts """
    qty_val: str = source_record.get('Qty_Current_Value', '')
    if qty_val not in ('', None) and is_number(qty_val):
        qty_val_num: float = float(qty_val)
        if qty_val_num.is_integer():
            qty_val = str(int(qty_val_num))
        else:
            qty_val = round(qty_val_num, 2)
    else:
        qty_val = ''

    subject_submitter_id: str = (f'COG_{source_record["NCH_Assigned_Patient_USI"]}').strip().upper()
    subject_submitter_id_count: int = subject_submitter_id_counts.get(subject_submitter_id)
    output_submitter_id: str = f'biospecimen_{subject_submitter_id}_{subject_submitter_id_count}'
    # set sort key e.g. 'biospecimen_COG_PABCDEF_1' => 'biospecimen_COG_PABCDEF_00001' for natural sort ordering
    sortkey: str = f'biospecimen_{subject_submitter_id}_{subject_submitter_id_count:05}'
    return {
        'sortkey': sortkey,
        'type': 'biospecimen',
        'project_id': project_id,
        '*submitter_id': output_submitter_id,
        '*subjects.submitter_id': subject_submitter_id,
        'biospecimen_container_type': source_record.get('Biospecimen_Unit_Type', ''),
        'biospecimen_media': source_record.get('Biospecimen_Media', ''),
        'biospecimen_type': source_record.get('Biospecimen_Type_Summary', ''),
        'current_qty_value': qty_val,
        'current_qty_unit': source_record.get('Qty_Current_UoM', '')
    }


def build_biospecimen_file(
    source_file_path: str,
    output_file_path: str,
    gen3_subjects: dict[str, dict[str , any]]
) -> None:
    """ Create TSV file for load into Gen3 portal from raw source JSON file """
    source_records: list[dict[str, any]] = []
    fd_data: typing.TextIO
    _logger.info('Loading source file "%s""', source_file_path)
    if not os.path.isfile(source_file_path):
        raise RuntimeError(f'Source file "{source_file_path}" not found')

    with open(source_file_path, encoding='utf-8') as fd_data:
        source_records = json.load(fd_data)
    if not source_records:
        raise RuntimeError(f'No source records found in source file "{source_file_path}"')

    # sort source records for consistent (idempotent) output for data-equivalent source files
    _logger.info('Sorting source records')
    source_records.sort(
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
    _logger.info('Processing source records')

    project_ids: set[str] = {v['project_id'] for v in gen3_subjects.values()}
    if len(project_ids) != 1:
        raise RuntimeError(f'Number of subject project_id values != 1: {project_ids}')

    project_id: str = project_ids.pop()
    subject_submitter_id_counts: dict[str, int] = {k:1 for k in gen3_subjects}
    subjects_found: set[str] = set()
    subjects_not_found: set[str] = set()

    num_source_records: int = len(source_records)
    num_source_records_processed: int = 0

    num_depleted_records: int = 0

    output_records: list[dict[str, any]] = []
    source_record: dict[str, any]
    for source_record in source_records:
        num_source_records_processed += 1
        if num_source_records_processed % 1000 == 0:
            _logger.info(
                '%d of %d source records processed, %d output records created',
                num_source_records_processed,
                num_source_records,
                len(output_records)
            )

        subject_submitter_id: str = (f'COG_{source_record["NCH_Assigned_Patient_USI"]}').strip().upper()

        # verify that source record is not depleted
        if source_record.get('Current_Status', '').upper() == 'DEPLETED':
            num_depleted_records += 1
            continue

        # verify that record subject exists in portal
        gen3_subject: dict[str, any] = gen3_subjects.get(subject_submitter_id, {})
        if not gen3_subject:
            if subject_submitter_id not in subjects_not_found:
                _logger.warning(
                    'Subject "%s" not found in Gen3 subject records, biospecimen record(s) not populated for subject',
                    subject_submitter_id
                )
                subjects_not_found.add(subject_submitter_id)
            continue

        subjects_found.add(subject_submitter_id)

        output_records.append(build_biospecimen_record(source_record, project_id, subject_submitter_id_counts))
        subject_submitter_id_counts[subject_submitter_id] += 1

    if not output_records:
        raise RuntimeError('No output records written')

    # sort records and remove sort key from final output records
    output_records.sort(key=lambda r: r['sortkey'])
    for output_record in output_records:
        output_record.pop('sortkey')

    # save biospecimen records to specified output path
    fd_tsv: typing.TextIO
    with open(output_file_path, mode='w', encoding='utf-8') as fd_tsv:
        writer: csv.DictWriter = csv.DictWriter(
            fd_tsv,
            fieldnames=output_records[0].keys(),
            delimiter='\t'
        )
        writer.writeheader()
        output_record: dict[str, any]
        for output_record in output_records:
            writer.writerow(output_record)
    _logger.info('Saved %d output records to "%s"', len(output_records), output_file_path)
    _logger.info(
        '%d source records processed, %d output records created, %d discarded because subject not found',
        num_source_records_processed,
        len(output_records),
        num_source_records - len(output_records)
    )
    _logger.info(
        '%d distinct subjects present in source records, %d subjects found in portal, %d subjects not found',
        len(subjects_found) + len(subjects_not_found),
        len(subjects_found),
        len(subjects_not_found)
    )
    if num_depleted_records:
        _logger.info('%d "DEPLETED" source record(s) excluded')


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
    output_file_path: str = './gen3_biospecimen.tsv'
    output_file_path_last: str = './gen3_biospecimen_last.tsv'
    if os.path.exists(output_file_path):
        if os.path.exists(output_file_path_last):
            os.remove(output_file_path_last)
        os.rename(output_file_path, output_file_path_last)
        if os.path.exists(output_file_path):
            raise RuntimeError(f'Unable to rename "{output_file_path}" to "{output_file_path_last}"')
    gen3_subject_file_paths: list[str] = json.loads(_CONFIG.get('GEN3_SUBJECT_FILE_PATHS', '[]'))
    gen3_subjects: dict[str, dict[str, any]] = get_gen3_subjects(gen3_subject_file_paths)
    build_biospecimen_file(local_data_file_path, output_file_path, gen3_subjects)
    if not os.path.exists(output_file_path):
        raise RuntimeError(f'Output file "{output_file_path}" not found, verify output file build was successful')


# AWS credentials and config needed in addition to config vars below:
# ~/.aws/credentials:
#[portal-staging]
#aws_access_key_id = ACCESS_KEY_ID
#aws_secret_access_key = SECRET_ACCESS_KEY

# ~/.aws/config:
#[profile nationwide-tissue-bank-staging]
#role_arn=arn:aws:iam::<nationwide data manager role id number>:role/nationwide-data-manager
#source_profile=portal-staging
_CONFIG: dict[str, any] = {
    'LOG_FILE_PATH': './get_nationwide_tissue_bank_data.log',
    'LOG_FILE_APPEND': False,
    'AWS_PROFILE_NAME': 'nationwide-tissue-bank-staging',
    'S3_BUCKET_NAME': 'nationwide-tissue-bank-data-staging',
    'S3_FILE_PREFIX': 'nationwide_data_',
    'USE_SAVED_SOURCE_DATA_FILE': True,
    'GEN3_SUBJECT_FILE_PATHS': [
        # specify in local env file e.g. .env to avoid having to hard-code local paths to gen3_subject.tsv files
        # containing subject submitter ids to be matched against subject ids ('NCH_Assigned_Patient_USI' property)
    ]
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

if __name__ == '__main__':
    main()
