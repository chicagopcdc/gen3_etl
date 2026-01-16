"""
Remove records from Gen3 platform
"""
import ast
import csv
import sys
import os
import logging
import json
import base64
import datetime
import requests

from gen3.auth import Gen3Auth
#from gen3.query import Gen3Query
from gen3.submission import Gen3Submission

import dotenv


class PortalRecordRemoverLogger:
    """
    Custom logger using python logging facility
    """
    def __init__(self, config: any) -> None:
        self._config: any = config
        self._logger = logging.getLogger(PortalRecordRemoverLogger.__name__)

        log_file_path: str = self._config.get('LOG_FILE_PATH', f'./{__name__}.log')

        log_file_append: bool = self._config.get('LOG_FILE_APPEND', 'false').lower() in ('true', '1')
        if not log_file_append and os.path.exists(log_file_path):
            os.remove(log_file_path)

        self._logger.addHandler(logging.FileHandler(log_file_path))
        self._logger.addHandler(logging.StreamHandler(sys.stdout))
        formatter: logging.Formatter = logging.Formatter(
            self._config.get('LOG_FORMAT', '%(asctime)s [%(levelname)s] %(message)s')
        )
        self._logger.setLevel(self._config.get('LOG_LEVEL', logging.INFO))
        handler: logging.Handler
        for handler in self._logger.handlers:
            handler.setFormatter(formatter)

    def critical(self, msg: str) -> None:
        """ log message with critical severity """
        self._logger.critical(msg)

    def debug(self, msg: str) -> None:
        """ log message with debug severity """
        self._logger.debug(msg)

    def error(self, msg: str) -> None:
        """ log message with error severity """
        self._logger.error(msg)

    def info(self, msg: str) -> None:
        """ log message with info severity """
        self._logger.info(msg)

    def warning(self, msg: str) -> None:
        """ log message with warning severity """
        self._logger.warning(msg)


class PortalRecordRemover:
    """
    Find and remove records from PCDC portal
    """
    NODE_TYPE_SUBJECT: str = 'subject'

    def __init__(self, config: dict[str, any], logger: PortalRecordRemoverLogger) -> None:
        self._config: dict[str, any] = config
        self._logger: PortalRecordRemoverLogger = logger
        self._credentials: str = self._config.get('CREDENTIALS')
        self._project: str = self._config.get('PROJECT')
        self._program_code: str = self._project.split('-')[0]
        self._project_code: str = self._project.split('-')[1]
        self._subject_filter: dict[str, str] = ast.literal_eval(self._config.get('SUBJECT_FILTER', '{}'))
        self._data_contributor_id: str = self._config.get('DATA_CONTRIBUTOR_ID')
        self._node_type: str = self._config.get('NODE_TYPE')
        self._portal_uuid_field: str = self._config.get('PORTAL_UUID_FIELD')
        self._dry_run_only: bool = self._config.get('DRY_RUN_ONLY', 'true').lower() in ('true', '1')

        self._delete_only_if_in_tsv_source: bool = self._config.get(
            'DELETE_ONLY_IF_IN_TSV_SOURCE', 'true'
        ).lower() in ('true', '1')
        self._tsv_source_data_file: str = self._config.get('TSV_SOURCE_DATA_FILE')
        self._tsv_source_record_id_field: str = self._config.get('TSV_SOURCE_RECORD_ID_FIELD')
        self._portal_source_record_id_field: str = self._config.get('PORTAL_SOURCE_RECORD_ID_FIELD')
        self._submit_records_source_json_file: str = self._config.get('SUBMIT_RECORDS_SOURCE_JSON_FILE')

        self._node_type_tsv_source_files: dict[str, str] = ast.literal_eval(
            self._config.get('NODE_TYPE_TSV_SOURCE_FILES', '{}')
        )

        # if requests ca bundle is specified then set OS env var so Gen3 API can read/use
        if self._config.get('REQUESTS_CA_BUNDLE', ''):
            os.environ['REQUESTS_CA_BUNDLE'] = self._config.get('REQUESTS_CA_BUNDLE', '')

        self._logger.info('Connecting with configured credentials:')
        self._logger.info(self.get_credential_properties())

        self._gen3_auth: Gen3Auth = Gen3Auth(refresh_file=self._credentials)
        self._gen3_submission: Gen3Submission = Gen3Submission(self._gen3_auth)

    @property
    def program_code(self) -> str:
        """ get config-specified program code, 'pcdc' """
        return self._program_code

    @property
    def project_code(self) -> str:
        """ get config-specified project code, ex: '20220808' """
        return self._project_code

    @property
    def subject_filter(self) -> str:
        """ get config-specified subject filter dictionary, ex: {'consortium': 'INRG'} """
        return self._subject_filter

    @property
    def node_type(self) -> str:
        """ get config-specified node type, ex: 'subject' """
        return self._node_type

    @property
    def portal_uuid_field(self) -> str:
        """ get config-specified portal uuid field, 'id' """
        return self._portal_uuid_field

    @property
    def dry_run_only(self) -> bool:
        """ get config-specified dry run indicator """
        return self._dry_run_only

    @property
    def delete_only_if_in_tsv_source(self) -> bool:
        """ get config-specified indicator of behavior to only delete records matched in source TSV """
        return self._delete_only_if_in_tsv_source

    @property
    def tsv_source_data_file(self) -> str:
        """ get config-specified TSV source file path, ex: '/Users/uid/workspace/pcdc-20220808/gen3_type.tsv' """
        return self._tsv_source_data_file

    @property
    def tsv_source_record_id_field(self) -> str:
        """ get config-specified TSV source record id field, ex: 'submitter_id' """
        return self._tsv_source_record_id_field

    @property
    def submit_records_source_json_file(self) -> str:
        """ get config-specified JSON source file path, ex: '/Users/uid/workspace/pcdc-20220808/import.json' """
        return self._submit_records_source_json_file

    @property
    def portal_source_record_id_field(self) -> str:
        """ get config-specified portal source record id field, ex: 'submitter_id' """
        return self._portal_source_record_id_field

    @property
    def node_type_tsv_source_files(self) -> dict[str, str]:
        """ get config-specified node type => source file mapping """
        return self._node_type_tsv_source_files

    def get_records(self, node_type: str, output_format: str='json') -> list:
        """ Return records (Gen3Submission.export_node()) from portal for specified type and format """
        self._logger.info('Retrieving records from portal')
        results: any = self._gen3_submission.export_node(
            program=self._program_code,
            project=self._project_code,
            node_type=node_type,
            fileformat=output_format
        )

        if 'errors' in results:
            self._logger.error('Error performing query:')
            self._logger.error(results['errors'])
            return []

        if 'data' not in results:
            self._logger.warning('\'data\' not returned in results')
            return []
        self._logger.info(f'{len(results["data"])} records retrieved')

        return results['data']

    def submit_records(self, records: any) -> any:
        """ Submit records (Gen3Submission.submit_record()) to portal """
        self._logger.info(f'Submitting {len(records)} records to portal')
        if self._dry_run_only:
            self._logger.info('Dry run specified, submit_record request not submitted to portal')
            return None

        return self._gen3_submission.submit_record(program=self._program_code, project=self._project_code, json=records)

    def delete_records(self, uuids: list) -> None:
        """ Delete records (Gen3Submission.delete_records()) from portal matching specified list of UUIDs """
        self._logger.info(f'Removing {len(uuids)} records from portal')
        if not self.dry_run_only:
            self._gen3_submission.delete_records(program=self._program_code, project=self._project_code, uuids=uuids)
        else:
            self._logger.info('Dry run specified, delete_records request not submitted to portal')

    def delete_node(self, node_type: str=None) -> None:
        """ Delete node type if specified, otherwise node type retrieved from config """
        node_name: str = node_type if node_type else self.node_type
        self._logger.info(f'Removing node type {node_name} for project {self._project_code} from portal')
        if not self.dry_run_only:
            self._gen3_submission.delete_node(
                program=self._program_code,
                project=self._project_code,
                node_name=node_name
            )
        else:
            self._logger.info('Dry run only specified, delete_node request not submitted to portal')

    def delete_nodes(self, node_types: list[str]=None) -> None:
        """ Delete node types if specified, otherwise node types retrieved from config """
        ordered_node_list: list[str] = node_types if node_types else ast.literal_eval(self.node_type)
        if not isinstance(ordered_node_list, list):
            self._logger.critical('List of node types to be deleted is missing/invalid')
            return

        self._logger.info(f'Removing node types for project {self._project_code} from portal: {ordered_node_list}')
        if not self.dry_run_only:
            self._gen3_submission.delete_nodes(
                program=self._program_code,
                project=self._project_code,
                ordered_node_list=ordered_node_list
            )
        else:
            self._logger.info('Dry run only specified, delete_nodes request not submitted to portal')

    def delete_project(self, project_code: str=None) -> None:
        """ Delete project if specified, otherwise project retrieved from config """
        project: str = project_code if project_code else self._project_code
        self._logger.info(f'Removing project {project} from portal')
        if not self.dry_run_only:
            self._gen3_submission.delete_project(program=self._program_code, project=project)
        else:
            self._logger.info('Dry run only specified, delete_project request not submitted to portal')

    def delete_program(self, program_name: str=None) -> None:
        """ Delete program if specified, otherwise program retrieved from config """
        program: str = program_name if program_name else self._program_code
        self._logger.info(f'Removing program {program} from portal')
        if not self.dry_run_only:
            self._gen3_submission.delete_program(program=self._program_code)
        else:
            self._logger.info('Dry run only specified, delete_program request not submitted to portal')

    def find_subject(self, subject_id: str, subjects: list) -> dict[str, any]:
        """ Find subject given specified id and list of subjects """
        subjects_found: list = list(filter(lambda s: s['id'] == subject_id, subjects))
        if len(subjects_found) > 1:
            self._logger.warning(f'{len(subjects_found)} subjects found having id \'{subject_id}\'')
            return None
        return subjects_found[0] if len(subjects_found) == 1 else None

    def decode_credentials(self) -> dict[str, any]:
        """ Decode portal credentials specified in config """
        with open(self._credentials, mode='r', encoding='utf-8') as credentials_file:
            credentials: dict = json.load(credentials_file)
            api_key = credentials['api_key']
            api_key_parts = api_key.split('.')
            if len(api_key_parts) != 3:
                raise RuntimeError('Invalid credential api key')
            # pad with '=' to ensure length is multiple of 4
            api_key_padded = api_key_parts[1] + "="*divmod(len(api_key), 4)[1]
            api_key_json_bytes = base64.urlsafe_b64decode(api_key_padded)
            api_key_json = json.loads(api_key_json_bytes)
            return api_key_json

    def get_credential_properties(self) -> str:
        """ Get properties of credentials specified in config """
        credential_properties: dict[str, any] = self.decode_credentials()
        return {
            'issuer': credential_properties['iss'],
            'issue_date': datetime.datetime.fromtimestamp(credential_properties['iat']).isoformat(),
            'expire_date': datetime.datetime.fromtimestamp(credential_properties['exp']).isoformat()
        }


def export_records(record_remover: PortalRecordRemover, logger: PortalRecordRemoverLogger) -> None:
    """ Export records from portal for program, project, and (optional) subject filter specified in config """
    logger.info('*** Portal record export started ***')

    # subject uuid => uuid if subject filter specified in config
    filter_subjects: dict[str, str] = {}

    # if filter (e.g. "{'consortium': 'INRG'}") specified then we need to load all subjects matching that
    # filter first since non-subject node records will be determined through their associated subject
    if record_remover.subject_filter:
        logger.info(f'Loading portal subjects matching filter \'{record_remover.subject_filter}\'')
        portal_subjects: list = record_remover.get_records(PortalRecordRemover.NODE_TYPE_SUBJECT)
        filter_subjects = {
            ps[record_remover.portal_uuid_field]:ps[record_remover.portal_uuid_field]
                for ps in portal_subjects if all(ps[k] == v for k,v in record_remover.subject_filter.items())
        }
        if len(filter_subjects) == 0:
            logger.info(f'No portal subjects matching filter "{record_remover.subject_filter}" found, aborting')
            return

        logger.info(f'{len(filter_subjects)} portal subjects matched filter "{record_remover.subject_filter}"')

    logger.info(
        f'Retrieving{" filtered" if record_remover.subject_filter else ""} ' +
        f'{record_remover.node_type} records from portal'
    )
    records: list = record_remover.get_records(node_type=record_remover.node_type, output_format='json')
    logger.info(f'{len(records)} {record_remover.node_type} record(s) retrieved from portal')

    exported_records: list = []

    # verify match by associated subject if filter specified in config
    if not filter_subjects:
        exported_records.extend(records)
    else:
        exported_records.extend(
            r for r in records if (
                record_remover.node_type == PortalRecordRemover.NODE_TYPE_SUBJECT and r['id'] in filter_subjects
                or
                'subjects' in r and r['subjects'][0]['node_id'] in filter_subjects
            )
        )
    if exported_records:
        logger.info('Saving %d exported records to export.txt', len(exported_records))
    else:
        logger.warning('No exported records to be saved')
        return

    exported_records.sort(key=lambda r:r[record_remover.portal_source_record_id_field])
    with open('export.txt', mode='w', encoding='utf-8') as export_fd:
        for exported_record in exported_records:
            export_fd.write(f'{json.dumps(exported_record)}\n')


def submit_records(record_remover: PortalRecordRemover, logger: PortalRecordRemoverLogger) -> None:
    """ Submit records to portal for program, project """
    logger.info('*** Portal record submission started ***')

    records: list = []
    if (not record_remover.submit_records_source_json_file or
        not os.path.exists(record_remover.submit_records_source_json_file)):
        logger.error(f'Source json file not found: {record_remover.submit_records_source_json_file}')
        return

    with open(record_remover.submit_records_source_json_file, encoding='utf-8') as input_file:
        records = json.load(input_file)

    batch_size: int = 100
    i: int = 0
    while i < len(records):
        index_end: int = min(i + batch_size, len(records))
        records_batch: list = records[i:index_end]
        for entity in records_batch:
            entity.pop('project_id', None)

        response: any = None
        try:
            response = record_remover.submit_records(records_batch)
        except (requests.HTTPError, requests.ConnectionError)  as exception:
            logger.error(f'Error submitting entities {i} => {index_end}')
            if 0 <= i < len(records) and 0 <= index_end < len(records):
                try:
                    logger.error(
                        f'Error submitting entities {records[i]["submitter_id"]} => ' +
                        f'{records[index_end]["submitter_id"]}'
                    )
                finally:
                    pass
            logger.error(exception)
            try:
                logger.error(exception.response.status_code)
                logger.error(exception.response.content)
            finally:
                pass

            if response:
                logger.debug(response)

        i = index_end
        if i % 1000 == 0:
            logger.info(f'{i} records processed')

    if i % 1000 != 0:
        logger.info(f'{i} records processed')


def remove_records(record_remover: PortalRecordRemover, logger: PortalRecordRemoverLogger) -> None:
    """ Remove records from portal for program, project and (optional) subject filter specified in config """
    logger.info('*** Portal record removal started ***')

    # subject uuid => consortium
    filter_subjects: dict[str, str] = {}

    # if filter (e.g. "{'consortium': 'INRG'}") specified then we need to load all subjects matching that
    # filter first since non-subject node records will be determined through their associated subject
    if record_remover.subject_filter:
        logger.info(f'Loading portal subjects matching filter \'{record_remover.subject_filter}\'')
        portal_subjects: list = record_remover.get_records(PortalRecordRemover.NODE_TYPE_SUBJECT)
        filter_subjects = {
            ps[record_remover.portal_uuid_field]:ps[record_remover.portal_uuid_field]
                for ps in portal_subjects if all(ps[k] == v for k,v in record_remover.subject_filter.items())
        }
        if len(filter_subjects) == 0:
            logger.info(f'No portal subjects matching filter "{record_remover.subject_filter}" found, aborting')
            return

        logger.info(f'{len(filter_subjects)} portal subjects matched filter "{record_remover.subject_filter}"')

    node_type_tsv_source_files: dict[str, str]
    node_type: str
    tsv_source_data_file: str
    delete_only_if_in_tsv_source: bool
    if not record_remover.node_type_tsv_source_files:
        node_type_tsv_source_files = { record_remover.node_type: record_remover.tsv_source_data_file }
        delete_only_if_in_tsv_source = record_remover.delete_only_if_in_tsv_source
    else:
        node_type_tsv_source_files = record_remover.node_type_tsv_source_files
        delete_only_if_in_tsv_source = True

    tsv_source_ids_to_remove: list = []
    for node_type, tsv_source_data_file in node_type_tsv_source_files.items():
        # if 'delete only if in tsv source' or type=>source file map specified
        # then load record ids ('submitter id') to delete from TSV
        tsv_source_ids_to_remove.clear()

        if delete_only_if_in_tsv_source:
            if node_type in ('program', 'project'):
                raise RuntimeError(f'Set "DELETE_ONLY_IF_IN_TSV_SOURCE" to false to delete the {node_type} node')

            logger.info(
                f'\'Delete only if in TSV source\' or source files specified, loading {node_type} source ids from TSV'
            )
            with open(tsv_source_data_file, encoding='utf-8') as tsv_fd:
                row: dict[str, any]
                rdr: list[dict[str, any]] = csv.DictReader(tsv_fd, dialect='excel-tab')
                for row in rdr:
                    if row[record_remover.tsv_source_record_id_field] not in tsv_source_ids_to_remove:
                        tsv_source_ids_to_remove.append(row[record_remover.tsv_source_record_id_field])
            logger.info(
                f'Loaded {len(tsv_source_ids_to_remove)} {node_type} unique source ids from TSV to remove from portal'
            )

        # get all records of specified type to remove from portal
        logger.info('Retrieving records to remove from portal')
        records: list = record_remover.get_records(node_type=node_type, output_format='json')
        logger.info(f'{len(records)} {node_type} record(s) retrieved from portal')

        source_ids_to_remove: list = (
            tsv_source_ids_to_remove
                if len(tsv_source_ids_to_remove) > 0
                else [r[record_remover.portal_source_record_id_field] for r in records]
        )

        records_dict: dict[str, any] = dict(
            (r[record_remover.portal_source_record_id_field], r) for r in records
        )

        # populate list of existing portal uuids to remove (all if 'delete only if in tsv source' set to false)
        logger.info(f'Loading portal {node_type} uuids to remove')
        portal_uuids_to_remove: list = []

        num_recs_processed: int = 0
        source_id_to_remove: str
        for source_id_to_remove in source_ids_to_remove:
            record: dict[str, any] = records_dict.get(source_id_to_remove, None)
            num_recs_processed += 1
            if num_recs_processed % 1000 == 0:
                logger.info(f'{num_recs_processed} records processed')

            if record is None:
                logger.warning(
                    f'{node_type} with {record_remover.portal_source_record_id_field} ' +
                    f'\'{source_id_to_remove}\' not found, skipping'
                )
                continue

            # verify match by associated subject if filter specified in config
            if filter_subjects:
                if node_type != PortalRecordRemover.NODE_TYPE_SUBJECT and 'subjects' in record:
                    if len(record['subjects']) != 1:
                        logger.warning(f'{len(record["subjects"])} subjects found for record, skipping:')
                        logger.warning(record)
                        continue

                    record_subject: dict[str, any] = record['subjects'][0]

                    if record_subject['node_id'] not in filter_subjects:
                        continue
                elif (
                    node_type == PortalRecordRemover.NODE_TYPE_SUBJECT
                    and
                    record[record_remover.portal_uuid_field] not in filter_subjects
                ):
                    continue

            # record qualifies for deletion, add to delete list
            portal_uuids_to_remove.append(record[record_remover.portal_uuid_field])

        logger.info(f'Loaded {len(portal_uuids_to_remove)} portal uuids to remove')

        if len(portal_uuids_to_remove) == 0:
            logger.info(f'No portal {node_type} records to be removed found, exiting')
            continue

        logger.info(f'{len(records) - len(portal_uuids_to_remove)} {node_type} records will remain')

        # remove records from portal in batches
        if not record_remover.dry_run_only:
            i: int = 0
            batch_size: int = 1000
            while i < len(portal_uuids_to_remove):
                record_remover.delete_records(uuids=portal_uuids_to_remove[i: i + batch_size])
                i += batch_size
                logger.info(f'{min(i, len(portal_uuids_to_remove))} records processed')
            logger.info(f'{len(portal_uuids_to_remove)} portal {node_type} records removed')
        else:
            logger.info(
                f'Dry run only specified, {len(portal_uuids_to_remove)} ' +
                f'{node_type} records will not be deleted from portal'
            )
            for gen3_uuid_to_remove in portal_uuids_to_remove:
                logger.debug(f'Portal record would have been removed: {gen3_uuid_to_remove}')

    logger.info('*** Portal record removal completed ***')


def remove_node(record_remover: PortalRecordRemover, logger: PortalRecordRemoverLogger) -> None:
    """ Remove all node/type records from portal for program and portal specified in config (for *all* consortiums) """
    logger.info('*** Portal single node removal started ***')

    if not record_remover.dry_run_only:
        record_remover.delete_node()
    else:
        logger.info(
            f'Dry run only specified, {record_remover.node_type} node will not be deleted ' +
            f'from portal for project {record_remover.project_code}'
        )

    logger.info('*** Portal single node removal completed ***')


def remove_nodes(record_remover: PortalRecordRemover, logger: PortalRecordRemoverLogger) -> None:
    """ Remove all node/type records from portal for program and portal specified in config (for *all* consortiums) """
    logger.info('*** Portal multiple node removal started ***')

    if not record_remover.dry_run_only:
        record_remover.delete_nodes()
    else:
        logger.info(
            f'Dry run only specified, {record_remover.node_type} nodes will not be deleted ' +
            f'from portal for project {record_remover.project_code}'
        )

    logger.info('*** Portal multiple node removal completed ***')


def remove_project(record_remover: PortalRecordRemover, logger: PortalRecordRemoverLogger) -> None:
    """ Remove project specified in config from portal """
    logger.info('*** Portal project removal started ***')

    if not record_remover.dry_run_only:
        record_remover.delete_project()
    else:
        logger.info(f'Dry run only specified, project {record_remover.project_code} will not be deleted from portal')

    logger.info('*** Portal project removal completed ***')

def remove_program(record_remover: PortalRecordRemover, logger: PortalRecordRemoverLogger) -> None:
    """ Remove program specified in config from portal """
    logger.info('*** Portal program removal started ***')

    if not record_remover.dry_run_only:
        record_remover.delete_program()
    else:
        logger.info(f'Dry run only specified, program {record_remover.program_code} will not be deleted from portal')

    logger.info('*** Portal program removal completed ***')

def show_credentials(record_remover: PortalRecordRemover, logger: PortalRecordRemoverLogger) -> None:
    """ Display properties of portal credentials specified in config """
    logger.info('*** Portal credential property retrieval started ***')
    credential_data: dict[str, any] = record_remover.decode_credentials()
    logger.info(credential_data)
    logger.info('*** Portal credential property retrieval completed ***')


def print_usage() -> None:
    """ Print usage for standalone """
    usage: str = (f'usage: {sys.argv[0]} [' +
        remove_records.__name__ + '|' +
        export_records.__name__ + '|' +
        remove_node.__name__ + '|' +
        remove_nodes.__name__ + '|' +
        remove_project.__name__ + '|' +
        remove_program.__name__ + '|' +
        show_credentials.__name__ +
    ']')
    print(usage)


def main() -> None:
    """ Entry point for standalone """
    callable_functions: dict[str, any] = {
        remove_records.__name__: remove_records,
        submit_records.__name__: submit_records,
        export_records.__name__: export_records,
        remove_node.__name__: remove_node,
        remove_nodes.__name__: remove_nodes,
        remove_project.__name__: remove_project,
        remove_program.__name__: remove_program,
        show_credentials.__name__: show_credentials
    }

    if len(sys.argv) == 2 and sys.argv[1] in callable_functions:
        config: any = dotenv.dotenv_values('.env')
        logger: PortalRecordRemoverLogger = PortalRecordRemoverLogger(config)
        record_remover: PortalRecordRemover = PortalRecordRemover(config, logger)

        callable_functions[sys.argv[1]](record_remover=record_remover, logger=logger)

    else:
        print_usage()


if __name__ == '__main__':
    main()
