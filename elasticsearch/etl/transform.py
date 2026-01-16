"""
Transform data extracted from graphdb (sheepdog/peregrine) into json format that can be loaded into Elasticsearch
"""
import csv
import json
import os
import sys
import logging
import copy
import requests
from dotenv import load_dotenv


load_dotenv('../.env')

logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False
if len(logger.handlers) > 0:
    logger.handlers.clear()
logger.addHandler(logging.StreamHandler(sys.stdout))
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
for handler in logger.handlers:
    handler.setFormatter(formatter)

NODE_TYPE_BIOSPECIMEN: str = 'biospecimen'
NODE_TYPE_PERSON: str = 'person'
NODE_TYPE_SUBJECT: str = 'subject'
NODE_TYPE_SURVIVAL_CHARACTERISTIC: str = 'survival_characteristic'
NODE_TYPE_TUMOR_ASSESSMENT: str = 'tumor_assessment'

# number fields that should be cleared in data portal if not specified in input data
UNSET_IF_NULL_FIELDS: dict[str, any] = {
    NODE_TYPE_SURVIVAL_CHARACTERISTIC: ['age_at_lkss'],
    NODE_TYPE_TUMOR_ASSESSMENT: [
        'longest_diam_dim1',
        'longest_diam_dim2',
        'longest_diam_dim3'
    ]
}

# $ref fields that need to be set that aren't array/enum/number/string, ex: subject=>project_id
REF_FIELDS: dict[str, str] = {
    NODE_TYPE_SUBJECT: ['project_id']
}

BIOSPECIMEN_STATUS_FIELD: str = f'{NODE_TYPE_BIOSPECIMEN}_status'
BIOSPECIMEN_STATUS_PRESENT: str = 'COG Biopathology Center'

# suppress or populate fields as specified in config, with allow taking precedence over deny if both specified
#   {
#       'subject.treatment_arm': {
#           'control_field': 'subject.consortium',
#           'allowed_control_field_values': ['INRG']
#           'blocked_control_field_values': []
#       }
#   }
SUPPRESSED_FIELDS: dict[str, any] = dict(json.loads(os.environ.get('SUPPRESSED_FIELDS', '{}')))

number_fields: list[str] = []
array_fields: list[str] = []
data_dictionary: dict[str, any] = {}

# node->field->var_type mapping to be used when performing graphdb to Elasticsearch data transform
# will be automatically populated from data dictionary; add explicit entries to override:
#   {'node type': {'field name': {'is_number': bool, 'unset_if_null': bool, 'is_array': bool}, ... }, ... }
node_type_fields_to_set: dict[str, any] = {}


def get_pluralized_node_type_name(node_type: str) -> str:
    """ Get pluralized form of node type name for update/mapping purposes """
    if node_type in ['molecular_analysis', 'secondary_malignant_neoplasm', 'submitted_unaligned_reads']:
        # pluralized subject collection property name is same as singular form
        return node_type

    if node_type[-1] == 'y':
        # histology => histologies, study => studies, etc
        return node_type[:-1] + 'ies'

    # lab => labs, etc
    return node_type + 's'


def load_data_dictionary(data_dict_url: str = None) -> dict[str, any]:
    """
    Load and return data dictionary from URL parameter or environment
    In terms of properties of interest, the dictionary is of the form:
    { "entity_type.yaml": { "properties": { "property_name": { "enum": { "value1" ... } ... } ... } ... } ... }

    for example:
    { "lab.yaml": { "properties": { ... "lab_spec_type": { "enum": { "Blood", ... } ... } ... } ... } ... }
    """
    data_dict_url: str = data_dict_url if data_dict_url else os.environ.get('DICTIONARY_URL', None)
    if not data_dict_url:
        raise RuntimeError('Unable to retrieve data dictionary, URL not specified')
    logger.info('Loading data dictionary from "%s"', data_dict_url)
    # bypass requests_ca_bundle to retrieve data dict from external (e.g. S3) source during local env ETL
    requests_ca_bundle: str = os.environ.get('REQUESTS_CA_BUNDLE', '')
    if requests_ca_bundle and 'localhost' not in data_dict_url.lower():
        os.environ['REQUESTS_CA_BUNDLE'] = ''
    response: requests.Response = requests.get(data_dict_url, timeout=180)
    if requests_ca_bundle and 'localhost' not in data_dict_url.lower():
        os.environ['REQUESTS_CA_BUNDLE'] = requests_ca_bundle
    if not response.ok:
        response.raise_for_status()
    data_dict: dict[str, any] = response.json()
    # re-key to remove '.yaml' suffix from node type e.g. tumor_assessment.yaml => tumor_assessment
    data_dict = {(key.replace('.yaml', '')):value for key,value in data_dict.items()}
    data_dictionary.clear()
    data_dictionary.update(data_dict)
    return data_dictionary


def load_field_type_lists() -> dict[str, any]:
    """
    Load the number and array field properties from the data dictionary
    """
    try:
        if not data_dictionary:
            load_data_dictionary()
            if not data_dictionary:
                raise RuntimeError('Data dictionary not populated')

        number_fields.clear()
        array_fields.clear()

        field: str
        field_properties: dict[str, any]
        node_type: str
        for node_type, node_type_properties in data_dictionary.items():
            # e.g. _definitions (skip), lab, etc
            if node_type.startswith('_') or 'properties' not in node_type_properties:
                continue

            if node_type not in node_type_fields_to_set:
                node_type_fields_to_set[node_type] = {}

            for field, field_properties in node_type_properties['properties'].items():
                field_type: any = field_properties['type'] if 'type' in field_properties else None
                is_number: bool = bool(
                    (isinstance(field_type, str) and field_type == 'number') or
                    (isinstance(field_type, list) and 'number' in field_type)
                )
                is_array: bool = bool(
                    (isinstance(field_type, str) and field_type == 'array') or
                    (isinstance(field_type, list) and 'array' in field_type)
                )
                if is_number:
                    number_fields.append(field)
                if is_array:
                    array_fields.append(field)

                # skip if already defined (overrided)
                if bool(node_type in node_type_fields_to_set and
                    field in node_type_fields_to_set[node_type] and
                    node_type_fields_to_set[node_type][field]):
                    continue

                if 'enum' in field_properties or (node_type in REF_FIELDS and field in REF_FIELDS[node_type]):
                    # enum, ref and string fields get treated as string fields (simple assignment of dest to src value)
                    node_type_fields_to_set[node_type][field] = {
                        'is_number': False,
                        'unset_if_null': False,
                        'is_array': False
                    }
                elif 'type' in field_properties:
                    node_type_fields_to_set[node_type][field] = {
                        'is_number': is_number,
                        'unset_if_null': bool(
                            ('*' in UNSET_IF_NULL_FIELDS and field in UNSET_IF_NULL_FIELDS['*'])
                            or
                            (node_type in UNSET_IF_NULL_FIELDS and field in UNSET_IF_NULL_FIELDS[node_type])
                        ),
                        'is_array': is_array
                    }

        # confirm that all node fields to set have been found and categorized in data dictionary
        node_type_fields_errors: bool = False
        for node_type, node_type_fields in node_type_fields_to_set.items():
            if not node_type_fields:
                node_type_fields_errors = True
                logger.error('Missing/incomplete node type field specification in data dictionary: %s', node_type)
            for field, field_spec in node_type_fields.items():
                if not field_spec:
                    node_type_fields_errors = True
                    logger.error(
                        'Missing/incomplete node type field specification in data dictionary: %s (%s)', node_type, field
                    )
        if node_type_fields_errors:
            raise RuntimeError('Missing/incomplete node type field specification in data dictionary')
    except requests.exceptions.HTTPError as http_error:
        logger.error('HTTPError loading field type lists:')
        logger.exception(http_error)
        raise
    except Exception as err:
        logger.error('Error loading field type lists:')
        logger.exception(err)
        raise


def to_num(val: any) -> any:
    """ Convert specified input value to number while trying to determine whether to return float or int """
    num_val: float = float(val)
    return int(num_val) if num_val.is_integer() else num_val


def to_array(val: any, delimiter: str = ',') -> list:
    """
    Convert specified input value to array by splitting on delimiter if needed. Default to comma (',') delimiter
    because gen3 submission's export_node() call returns array fields as comma-separated strings instead of arrays
    """
    try:
        # csv.reader() expects and returns list of lines e.g. [[value1, value2]]
        return next(csv.reader(val.splitlines(), delimiter=delimiter)) if not isinstance(val, list) else val
    except ValueError as verr:
        logger.error('Error transforming in array value: %s', val)
        logger.error(verr)
        raise


def get_timings_by_subject_id(data: dict[str, any]) -> dict[str, list[dict[str, any]]]:
    """ Load and return timing records from JSON data exported from graphdb (sheepdog/peregrine) """
    timings_by_subject_id: dict[str, any] = {}
    logger.info('Loading timing records...')

    # field_name => convert_to_num
    optional_fields: dict[str, bool] = {
        'age_at_course_end': True,
        'age_at_course_start': True,
        'age_at_disease_phase': True,
        'course': False,
        'course_number': True,
        'disease_phase': False,
        'disease_phase_number': True,
        'timing_type': False,
        'year_at_disease_phase': True
    }
    field: str
    for field in optional_fields:
        optional_fields[field] = bool(field in number_fields)
    project: dict[str, any]
    for project in data.values():
        record: dict[str, any]
        for record in project['timing']:
            timing: dict[str, any] = {}

            # required system attributes
            timing['_timing_id'] = record['id']
            timing['timing_id'] = record['submitter_id']

            field: str
            convert_to_num: bool
            for field, convert_to_num in optional_fields.items():
                if field in record and record[field]:
                    timing[field] = record[field] if not convert_to_num else to_num(record[field])

            if 'subjects' in record and record['subjects']:
                subject: dict[str, any]
                for subject in record['subjects']:
                    if subject['node_id'] not in timings_by_subject_id:
                        timings_by_subject_id[subject['node_id']] = []
                    timings_by_subject_id[subject['node_id']].append(copy.deepcopy(timing))

    return timings_by_subject_id


def get_persons_by_person_id(data: dict[str, any]) -> dict[str, dict[str, any]]:
    """ Load and return person records from JSON data exported from graphdb (sheepdog/peregrine) """
    persons_by_person_id: dict[str, any] = {}
    logger.info('Loading person records...')

    # field_name => convert_to_num
    optional_fields: list[str] = {
        'ethnicity': False,
        'race': False,
        'sex': False
    }
    field: str
    for field in optional_fields:
        optional_fields[field] = bool(field in number_fields)
    project: dict[str, any]
    for project in data.values():
        record: dict[str, any]
        for record in project[NODE_TYPE_PERSON]:
            person: dict[str, any] = {}

            # required system attributes
            person['_person_id'] = record['id']
            person['person_id'] = record['submitter_id']

            field: str
            convert_to_num: bool
            for field, convert_to_num in optional_fields.items():
                if field in record and record[field]:
                    person[field] = record[field] if not convert_to_num else to_num(record[field])

            persons_by_person_id[person['_person_id']] = person

    return persons_by_person_id


def find_record_by_id(id_value: any, id_field: str, records: list[dict[str, any]]) -> dict[str, any]:
    """ Find and return record for specified record set, id field and value """
    # 1) expectedResult = [d for d in exampleSet if d['type'] in keyValList]
    # 2) list(filter(lambda d: d['type'] in keyValList, exampleSet))

    matches: list = [r for r in records if r[id_field] == id_value]

    num_result = len(matches)
    if num_result == 1:
        return matches[0]

    if num_result == 0:
        logger.warning('ERROR - unable to find record with id field "%s" having value "%s"', id_field, id_value)
    else:
        logger.info('ERROR - too many matches found for id field "%s" having value "%s"', id_field, id_value)

    return None


def has_timing_association(node_type: str) -> bool:
    """ Determine whether specified node type has timing association deefined in data dictionary) """
    if node_type not in data_dictionary:
        msg: str = f'Unable to determine timing association, node type "{node_type}" not found in data dictionary'
        logger.fatal(msg)
        raise RuntimeError(msg)

    return bool(
        'links' in data_dictionary[node_type] and
        [l for l in data_dictionary[node_type]['links'] if 'name' in l and l['name'] == 'timings']
    )


def set_timing_fields(node_record: dict[str, any], timing_event: dict[str, any]) -> None:
    """ Set timing fields for specified node record and timing event """
    fields: list[str] = [
        '_timing_id',
        'age_at_course_end',
        'age_at_course_start',
        'age_at_disease_phase',
        'course',
        'course_number',
        'disease_phase',
        'disease_phase_number',
        'timing_id',
        'timing_type',
        'year_at_disease_phase'
    ]
    field: str
    for field in fields:
        if field in timing_event and timing_event[field]:
            node_record[field] = timing_event[field]


def sort_and_flatten_survival_characteristics(survival_characteristics: list[dict[str, any]]) -> list[dict[str, any]]:
    """
    Sort and flatten survival characteristic records so each subject has at most one record by following methodlogy:
    - for each subject:
        - sort survival_characteristic records from top to bottom by age_at_lkss descending
        - bring top-most survival_characteristic record having lkss = 'Dead' to top if found
        - discard remaining survival_characteristic records
    """
    logger.info('Sorting and flattening survival characteristics')
    # perform basic validation and map subject ids to survival characteristics for fast lookup
    log_msg: str
    survival_characteristics_by_subject: dict[str, list[dict[str, any]]] = {}
    survival_characteristic: dict[str, any] = None
    subject: dict[str, any] = None
    subject_id: str
    for survival_characteristic in survival_characteristics:
        if 'type' not in survival_characteristic or survival_characteristic['type'] != NODE_TYPE_SURVIVAL_CHARACTERISTIC:
            invalid_type: str = survival_characteristic['type'] if type in survival_characteristic else ''
            log_msg = f'Error flattening survival_characteristic records: unexpected type \'{invalid_type}\''
            logger.fatal(log_msg)
            logger.fatal(survival_characteristic)
            raise TypeError(log_msg)

        if 'subjects' not in survival_characteristic or not survival_characteristic['subjects']:
            log_msg = 'Error flattening survival_characteristic records: \'subjects\' missing or empty'
            logger.fatal(log_msg)
            raise RuntimeError(log_msg)

        for subject in survival_characteristic['subjects']:
            if 'submitter_id' not in subject or not subject['submitter_id']:
                log_msg = 'Error flattening survival_characteristic records: subject submitter_id missing or invalid'
                logger.fatal(log_msg)
                raise RuntimeError(log_msg)

            subject_id = subject['submitter_id']
            if subject_id not in survival_characteristics_by_subject:
                survival_characteristics_by_subject[subject_id] = []
            survival_characteristics_by_subject[subject_id].append(survival_characteristic)

    # enumerate mapped subject/survival characteristic records and populate flattened collection to be returned
    flattened_survival_characteristics: list[dict[str, any]] = []
    subject_survival_characteristics: list[dict[str, any]]
    for subject_id, subject_survival_characteristics in survival_characteristics_by_subject.items():
        try:
            # sort by age_at_lkss (by presence then value) in descending order
            ordered_survival_characteristics: list[dict[str, any]] = sorted(
                subject_survival_characteristics,
                key=lambda ssc: ('age_at_lkss' in ssc, ssc.get('age_at_lkss', 0)),
                reverse=True
            )
            # elevate top-most survival characteristic having lkss=Dead
            dead_survival_characteristic: dict[str, any] = next(
                iter([osc for osc in ordered_survival_characteristics if 'lkss' in osc and osc['lkss'] == 'Dead']),
                None
            )
            # set preferred survival characteristic record for subject to be record
            # with lkss=Dead if present else record with max age_at_lkss
            preferred_survival_characteristic: dict[str, any] = (
                dead_survival_characteristic or next(iter(ordered_survival_characteristics), None)
            )
            flattened_survival_characteristics.append(preferred_survival_characteristic)
        except Exception as ex: # pylint: disable=broad-exception-caught
            logger.fatal('Error flattening survival characteristic for subject %s:', subject_id)
            logger.fatal(ex)
            logger.fatal('subject survival characteristics:')
            logger.fatal(subject_survival_characteristics)
            raise
    logger.info(
        'Sorted and flattended survival characteristics (%d => %d records)',
        len(survival_characteristics),
        len(flattened_survival_characteristics)
    )
    return flattened_survival_characteristics


def can_populate_node_record_field(
        node_type: str,
        field_name: str,
        source_record: dict[str, any],
        subjects: dict[str, dict[str, any]]
) -> bool:
    """
    Determine whether field can be populated for given node type and source record based on config, e.g.
    treatment_arm only set if subject consortium (consortia possible in theory) in treatment_arm allow list.
    {
        'study.treatment_arm': {
            'control_field': 'subject.consortium',
            'allowed_control_field_values': ['INRG']
            'blocked_control_field_values': []
        },
        ...
    }
    """
    if (field_name not in SUPPRESSED_FIELDS and
        f'{node_type}.{field_name}' not in SUPPRESSED_FIELDS and
        f'*.{field_name}' not in SUPPRESSED_FIELDS):
        # no suppression rule for input type or field, allow population
        return True

    # check all consortia for associated subject, whether directly if subject
    # record or from 'subjects' child property if non-subject record type
    suppressed_type_field: str = next(iter(SUPPRESSED_FIELDS))
    rule_spec: dict[str, any] = SUPPRESSED_FIELDS.get(suppressed_type_field, {})
    if ('control_field' not in rule_spec or
        'allowed_control_field_values' not in rule_spec or
        'blocked_control_field_values' not in rule_spec):
        raise RuntimeError('Invalid field suppression rule, missing required control field key(s)')

    # subject.treatment_arm => 'subject', 'treatment_arm'
    suppressed_type: str = (
        suppressed_type_field.split('.')[0] if '.' in suppressed_type_field else '*'
    ).strip()
    suppressed_field: str = (
        suppressed_type_field.split('.')[-1] if '.' in suppressed_type_field else suppressed_type_field
    ).strip()
    if suppressed_field != field_name or (suppressed_type not in (node_type, '*')):
        # shouldn't get here
        raise RuntimeError(
            f'Unexpected mismatch between input ("{node_type}.{field_name}") and ' +
            f'suppressed rule ("{suppressed_type}.{suppressed_field}") field'
        )

    # subject.consortium => 'subject', 'consortium'
    control_type: str = (
        rule_spec['control_field'].split('.')[0] if '.' in rule_spec['control_field'] else '*'
    ).strip()
    control_field: str = (
        rule_spec['control_field'].split('.')[-1] if '.' in rule_spec['control_field'] else '*'
    ).strip()

    allowed_values: set = set(rule_spec['allowed_control_field_values'])
    blocked_values: set = set(rule_spec['blocked_control_field_values'])

    # check for wildcard values, allow takes precendence
    if '*' in (rule_spec.get('allowed_control_field_values', None) or []):
        return True
    if '*' in (rule_spec.get('blocked_control_field_values', None) or []):
        return False

    record_control_values: set[str] = set()
    if node_type == control_type:
        # control field value to check is in direct child field
        record_control_values.add(source_record.get(control_field, '').strip())

    # e.g. non-subject control field will be child collection with pluralized
    # name such as 'lab'=>'labs', 'histology'=>'histologies', etc
    source_subject_control_child_field: str = get_pluralized_node_type_name(control_type)
    source_subject: dict[str, any]
    sparse_source_subject: dict[str, any]
    # collect control field values across all records for same subject
    for sparse_source_subject in source_record.get('subjects', []):
        source_subject = subjects.get(sparse_source_subject['submitter_id'])
        if control_type == NODE_TYPE_SUBJECT:
            record_control_values.add(source_subject.get(control_field, '').strip())
            continue

        source_subject_field: str
        source_subject_value: any
        source_subject_control_record: dict[str, any]
        for source_subject_field, source_subject_value in source_subject.items():
            if (
                (not isinstance(source_subject_value, list))
                or
                (control_type != '*' and source_subject_field != source_subject_control_child_field)
            ):
                continue
            # e.g. for each survival_characteristic record in subject.survival_characteristics
            for source_subject_control_record in source_subject_value:
                record_control_values.add(source_subject_control_record.get(control_field, '').strip())

    # check allowed values
    if allowed_values:
        return not record_control_values.isdisjoint(allowed_values)

    # check deny/block values
    if blocked_values:
        return record_control_values.isdisjoint(blocked_values)

    # allow population by default if no rules matched
    return True


def populate_node_record(
    node_type: str,
    source_record: dict[str, any],
    new_subjects: dict[str, dict[str, any]] = None,
    source_timings: dict[str, list[dict[str, any]]] = None,
    problematic_records: list[dict[str, any]] = None,
    set_base_fields_only: bool = False
) -> dict[str, any]:
    """ Populate (subject child) node record with data from source record for specified type """
    if node_type not in node_type_fields_to_set:
        #logger.warning('Type %s not found in node type field dictionary, skipping record population', node_type)
        return None

    new_record: dict[str, any] = {}

    field: str
    for field, field_spec in node_type_fields_to_set[node_type].items():
        try:
            if (source_record.get(field, None) is not None and
                can_populate_node_record_field(node_type, field, source_record, new_subjects)):
                # field present in source, has value specified (watch out for 'falsy' values like 0),
                # and allowed to populate (treatment_arm blocked for INSTRuCT), set on new record
                value: any = source_record[field]
                if field_spec['is_number']:
                    value = to_num(value)
                elif field_spec['is_array']:
                    value = to_array(value)
                new_record[field] = value
            elif field_spec['unset_if_null']:
                # field not present in source or no value specified, explictly unset on new record if present in list
                new_record[field] = None
        except:
            logger.error('%s: error setting field %s', node_type, field)
            logger.error('%s: field spec: %s', node_type, field_spec)
            logger.error(source_record)
            raise

    # map lkss to lkss_obfuscated for survival_characteristic:
    # Unknown=>Unknown, null/blank=>null/blank, not null/blank and not Unknown (Alive/Dead/etc) => Known
    if node_type == NODE_TYPE_SURVIVAL_CHARACTERISTIC and 'lkss' in new_record:
        new_record['lkss_obfuscated'] = (
            'Known'
                if new_record['lkss'] and new_record['lkss'] != 'Unknown' else
                new_record['lkss']
        )

    if set_base_fields_only:
        return new_record

    # populate timing fields and then attach to parent subject; set parent subject biospecimen status if needed
    source_record_subject: dict[str, any]
    for source_record_subject in source_record['subjects']:
        new_subject: dict[str, any] = new_subjects.get(source_record_subject['submitter_id'])
        if new_subject is None:
            logger.warning(
                'Newly added subject not found for "%s" subject submitter_id "%s", skipping',
                node_type,
                source_record_subject['submitter_id']
            )
            problematic_records.append(new_record)
            continue

        if has_timing_association(node_type) and source_record_subject['node_id'] not in source_timings:
            logger.warning(
                '"%s" record "%s": no timing records found for subject with node_id "%s" (subject submitter id "%s")',
                node_type,
                source_record['submitter_id'],
                source_record_subject['node_id'],
                source_record_subject['submitter_id']
            )

        if 'timings' in source_record:
            if len(source_record['timings']) > 1:
                logger.info('Too many timings associated with this "%s" record', node_type)
                problematic_records.append(source_record)
            else:
                subject_source_timings: list[dict[str, any]] = source_timings[source_record_subject['node_id']]
                source_record_timing: dict[str, any]
                for source_record_timing in source_record['timings']:
                    subject_source_timing = find_record_by_id(
                        source_record_timing['node_id'],
                        '_timing_id',
                        subject_source_timings
                    )
                    if subject_source_timing:
                        set_timing_fields(new_record, subject_source_timing)

        subject_property_name: str = get_pluralized_node_type_name(node_type)
        if subject_property_name not in new_subject:
            new_subject[subject_property_name] = []
        new_subject[subject_property_name].append(new_record)

        if node_type == NODE_TYPE_BIOSPECIMEN:
            # biospecimen record(s) found for parent subject so set biospecimen status field
            new_subject[BIOSPECIMEN_STATUS_FIELD] = BIOSPECIMEN_STATUS_PRESENT

    return new_record


def create_subject_record(
    source_record: dict[str, any],
    source_persons: dict[str, dict[str, any]],
    source_timings: dict[str, list[dict[str, any]]],
    problematic_records: list[dict[str, any]]
) -> dict[str, any]:
    """ Populate subject record with data from source record """
    subject: dict[str, any] = {}

    # system attributes
    subject['_subject_id'] = source_record['id']
    subject['subject_submitter_id'] = source_record['submitter_id']

    # populate base fields
    subject.update(populate_node_record(NODE_TYPE_SUBJECT, source_record, set_base_fields_only=True))

    if 'persons' in source_record:
        if len(source_record['persons']) > 1:
            logger.warning(
                'Too many person records associated to subject %s (%s)',
                subject['_subject_id'],
                subject['subject_submitter_id']
            )
            problematic_records.append(subject)
            return None

        person: dict[str, any]
        for person in source_record['persons']:
            subject.update(copy.deepcopy(source_persons[person['node_id']]))
            program_name: str
            project_code: str
            (program_name, project_code) = subject['project_id'].split('-')
            subject['auth_resource_path'] = (
                '/programs/' + program_name +
                '/projects/' + project_code +
                '/persons/' + subject['person_id'] +
                '/subjects/' + subject['subject_submitter_id']
            )

    ### TEMPORARY PATCH TO INCLUDE YEAR_AT_DISEASE_PHASE
    if subject['_subject_id'] in source_timings:
        # set subject year_at_disease_phase field with value from last subject timing record having valid value
        subject_timings: list[dict[str, any]] = list(source_timings[subject['_subject_id']])
        subject_timings_yadp: list[dict[str, any]] = [
            t for t in subject_timings if 'year_at_disease_phase' in t and t['year_at_disease_phase']
        ]
        if subject_timings_yadp:
            subject['year_at_disease_phase'] = subject_timings_yadp[-1]['year_at_disease_phase']
    ### END PATCH

    return subject


def generate_subject_json(data: dict[str, any], node_types: list[str]) -> list[dict[str, any]]:
    """ Transform specified json input data extracted from graphdb into subject-oriented json data set """
    # node_types = list(filter(lambda k: 'timing' not in k, node_types))
    node_types: list[str] = list(filter(lambda t: NODE_TYPE_PERSON not in t, node_types))

    problematic_records: list[dict[str, any]] = []
    subjects: dict[str, dict[str, any]] = {}

    load_field_type_lists()
    timings: dict[str, list[dict[str, any]]] = get_timings_by_subject_id(data=data)
    persons: dict[str, dict[str, any]] = get_persons_by_person_id(data=data)

    project: str
    project_records: dict[str, any]
    for project, project_records in data.items():
        logger.info('Generating subject record set for project %s', project)
        for node_type in node_types:
            logger.info('Processing node type %s', node_type)

            if node_type not in project_records:
                logger.warning('Node type %s not in record set, skipping', node_type)
                continue

            record_num: int = 0
            subject: dict[str, any]
            record: dict[str, any]
            node_type_records: list[dict[str, any]] = (
                project_records[node_type]
                    if node_type != NODE_TYPE_SURVIVAL_CHARACTERISTIC
                    else sort_and_flatten_survival_characteristics(project_records[node_type])
            )
            for record in node_type_records:
                record_num += 1
                if record_num % 1000 == 0:
                    logger.info('%d %s records processed', record_num, node_type)

                if node_type == NODE_TYPE_SUBJECT:
                    subject: dict[str, any] = create_subject_record(record, persons, timings, problematic_records)
                    if subject and subject['subject_submitter_id'] not in subjects:
                        subjects[subject['subject_submitter_id']] = subject
                    elif subject:
                        raise RuntimeError(
                            f'Duplicate subject submitter id found: "{subject["subject_submitter_id"]}"'
                        )
                else:
                    if 'subjects' not in record:
                        continue

                    if len(record['subjects']) > 1:
                        logger.info('Too many subjects associated to this "%s" record', node_type)
                        problematic_records.append(record)
                        continue

                    populate_node_record(node_type, record, subjects, timings, problematic_records)

    logger.info('%d subject records generated', len(subjects))
    logger.info('%d problem records found', len(problematic_records))
    return list(subjects.values())


def get_es_index_mapping_elem(elem_type: str, elem_sub_type: str = None) -> dict[str, any]:
    """ Get ES index mapping element for specified element type """
    elem: dict[str, any] = {'type': elem_type}
    if elem_type == 'keyword':
        elem['fields'] = {
            'analyzed': {
                'type': elem_sub_type if elem_sub_type else 'text'
            }
        }
    elif elem_type == 'nested':
        elem['properties'] = {}
    elif elem_type not in ['float']:
        raise ValueError(f'Unsupported element type: {elem_type}')
    return elem


def get_es_index_mapping_elem_type_name(data_dict_field_props: dict[str, any]) -> str:
    """ Get ES mapping element type name ('keyword' or 'float') for specified data dictionary field properties """
    if 'enum' in data_dict_field_props:
        return 'keyword'

    if 'type' not in data_dict_field_props:
        return None

    field_type: any = data_dict_field_props['type']

    # age_at_*/year_at_*, etc
    if bool(
        (isinstance(field_type, str) and field_type == 'number') or
        (isinstance(field_type, list) and 'number' in field_type)
    ):
        return 'float'

    # enrolled_status, chromosome, etc
    keyword_elem_type: str
    for keyword_elem_type in ('array', 'string'):
        if bool(
            (isinstance(field_type, str) and field_type == keyword_elem_type) or
            (isinstance(field_type, list) and keyword_elem_type in field_type)
        ):
            return 'keyword'

    return None


def get_es_index_mapping_timing_fields() -> dict[str, any]:
    """ Get ES mapping timing fields to be set for subject sub-types like lab etc based on data dictionary """
    if 'timing' not in data_dictionary or 'properties' not in data_dictionary['timing']:
        msg: str = 'Unable to find timing or timing->properties in data dictionary'
        logger.fatal(msg)
        raise RuntimeError(msg)

    es_mapping_timing_fields: dict[str, any] = {
        '_timing_id': get_es_index_mapping_elem('keyword'),
        'timing_type': get_es_index_mapping_elem('keyword')
    }
    for field, field_properties in dict(sorted(data_dictionary['timing']['properties'].items())).items():
        es_mapping_elem_type: str = get_es_index_mapping_elem_type_name(field_properties)
        if es_mapping_elem_type:
            es_mapping_timing_fields[field] = get_es_index_mapping_elem(es_mapping_elem_type)
    return es_mapping_timing_fields


def generate_es_index_mapping() -> dict[str, any]:
    """ Generate field mapping document to be loaded with data when creating Elasticsearch index """
    try:
        if not data_dictionary:
            load_data_dictionary()
            if not data_dictionary:
                raise RuntimeError('Data dictionary not populated')

        fields_to_skip: tuple[str, ...] = ('type', 'submitter_id')

        es_mapping_timing_fields: dict[str, any] = get_es_index_mapping_timing_fields()
        es_mapping_timing_fields = {k:v for k,v in es_mapping_timing_fields.items() if k not in fields_to_skip}
        if not es_mapping_timing_fields:
            raise RuntimeError('No timing fields mapped from data dictionary to ES index')

        es_mapping: dict[str, any] = {'mappings': {'properties' : {} }}

        node_type: str
        node_type_props: dict[str, any]

        # process subject and person first, then remaining types in alphabetical order
        sorted_data_dict: dict[str, any] = {
            NODE_TYPE_SUBJECT: data_dictionary[NODE_TYPE_SUBJECT],
            NODE_TYPE_PERSON: data_dictionary[NODE_TYPE_PERSON]
        }
        sorted_data_dict.update(dict(sorted(data_dictionary.items())))
        for node_type, node_type_props in sorted_data_dict.items():
            # e.g. _definitions (skip), timing, subject, lab, etc
            if node_type.startswith('_') or 'properties' not in node_type_props:
                continue

            node_type_pluralized: str = get_pluralized_node_type_name(node_type)

            target_elem: dict[str, any] = es_mapping['mappings']['properties']

            # properties for person and subject get merged together within root 'properties' element,
            # other types have their properties set in child 'nested' elements
            if node_type not in (NODE_TYPE_PERSON, NODE_TYPE_SUBJECT):
                target_elem[node_type_pluralized] = get_es_index_mapping_elem('nested')
                target_elem = target_elem[node_type_pluralized]['properties']

            # every node_type gets immediate child _node_type_id property
            target_elem[f'_{node_type}_id'] = get_es_index_mapping_elem('keyword')

            if node_type == NODE_TYPE_SUBJECT:
                target_elem['_molecular_analysis_count'] = get_es_index_mapping_elem('float')
                target_elem['_study_count'] = get_es_index_mapping_elem('float')
                target_elem['auth_resource_path'] = get_es_index_mapping_elem('keyword')
                target_elem['biospecimen_status'] = get_es_index_mapping_elem('keyword')
                target_elem['person_id'] = get_es_index_mapping_elem('keyword')
                target_elem['project_id'] = get_es_index_mapping_elem('keyword')
                target_elem['subject_submitter_id'] = get_es_index_mapping_elem('keyword')
                target_elem['year_at_disease_phase'] = get_es_index_mapping_elem('float')

            # es-only property to map obfuscated lkss values (Alive/Dead/etc=>Known, Unknown=>Unknown)
            if node_type == NODE_TYPE_SURVIVAL_CHARACTERISTIC:
                target_elem['lkss_obfuscated'] = get_es_index_mapping_elem('keyword')

            field: str
            field_props: dict[str, any]
            # define properties for data dictionary fields that are array
            # (formerly survival_characteristic.cause_of_death), enum (subject.consortium),
            # number (lab.age_at_lab), or string (vital.vitals_result)
            # skip fields specific to data dictionary like *.type and *.submitter id
            for field, field_props in {
                k:v for k,v in node_type_props['properties'].items() if k not in fields_to_skip
            }.items():
                # skip fields specific to data dictionary like type and submitter id
                # consortium, tumor_site, etc
                es_mapping_elem_type_name: str = get_es_index_mapping_elem_type_name(field_props)
                if es_mapping_elem_type_name:
                    target_elem[field] = get_es_index_mapping_elem(es_mapping_elem_type_name)

            # add _timing_id and data dict timing properties if node type has timing association
            if has_timing_association(node_type):
                for timing_field, timing_field_elem in es_mapping_timing_fields.items():
                    target_elem[timing_field] = timing_field_elem

        return es_mapping
    except requests.exceptions.HTTPError as http_error:
        logger.error('HTTPError retrieving data dictionary JSON:')
        logger.exception(http_error)
        raise
    except Exception as err:
        logger.error('Error retrieving data dictionary JSON:')
        logger.exception(err)
        raise
