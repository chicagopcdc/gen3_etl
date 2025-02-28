import requests
import logging
import os
from dotenv import load_dotenv
from transform import load_data_dictionary, get_es_index_mapping_timing_fields
from sys import exit, stdout
load_dotenv('../.env')
import json
import re
from manual_ES_to_DD_values import config
import argparse

logger: logging.Logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
logger.propagate = False
if len(logger.handlers) > 0:
    logger.handlers.clear()
logger.addHandler(logging.StreamHandler(stdout))
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
for handler in logger.handlers:
    handler.setFormatter(formatter)



BASE_URL = os.getenv("BASE_URL")

OUTPUT_FILE = os.getenv("OUTPUT_FILE")

es_mapping: dict[str, any]
dd_mapping: dict[str, any]

try:
    """
    load es mapping
    """
    es_mapping = requests.post(f"{BASE_URL}/guppy/graphql", json={"query":"{ _mapping { subject } }"}).json()["data"]["_mapping"]["subject"]
    es_mapping = sorted(es_mapping, key=lambda x: ('.' in x, x))
except Exception as e:
    logger.error(f"Error loading elastic search data: {e}")
    exit(1)

try: 
    """
    load data dictionary

    the url used is DICTIONARY_URL set in the .env file

    The scirpt can use one of the data dictionary links 

    or BASE_URL + /api/v0/submission/_dictionary/_all to use the currently ingested data dictionary

    """
    dd_mapping = load_data_dictionary()
except Exception as e:
    logger.error(f"Error loading data dictionary: {e}")
    exit(1)

"""
currently no word follows this in the data dictionary but added it
as a safety measure for the future
For instance ties goes to tie not ty
"""
singular_words_that_end_in_ie = {k for k in dd_mapping.keys() if k[-2:] == "ie"}


"""
These values are hardcoded in transform and required here
"""
fields_to_skip: tuple[str, ...] = ('type', 'submitter_id')

fields_with_unique_patterns = {'person_id', 'subject_submitter_id', 'timing_id'}

singular_plural_same = ['molecular_analysis', 'secondary_malignant_neoplasm', 'submitted_unaligned_reads']


"""
collect person, subject and timing fields
"""
person_fields = {k for k in dd_mapping['person']["properties"].keys() if k not in ["type"]}
for person_field in person_fields:
    logger.info(f"Person field: {person_field}")

subject_fields = {k for k in dd_mapping['subject']["properties"].keys() if k not in ["type"]}
for subject_field in subject_fields:
    logger.info(f"Subject field: {subject_field}")

timing_fields = get_es_index_mapping_timing_fields()
timing_fields = {k for k in timing_fields.keys() if k not in fields_to_skip}
for timeing_field in timing_fields:
    logger.info(f"Timing field: {timeing_field}")

"""
keep track of which node from gupppy the script is on for logging purposes
"""
current_node_type = None
def make_singular(node_type: str) -> str:
    """ 
    Get singular form of node type name for mapping back to data dictionary
    1) check if node_type is in the list of words where singular and plural are the same
    2) check if node_type ends in 'ies'
        a) if word ends in 'ies' check singular form ends in 'ie'
        b) otherwise remove 'ies' and add 'y'
    3) remove 's' from node_type
    """
    
    global current_node_type
     

    if node_type in singular_plural_same:
        message = f"Node type {node_type} is in the list of words where singular and plural are the same"
        singular_value = node_type
    
    elif node_type[-3:] == 'ies':
        if node_type[:-1] in singular_words_that_end_in_ie:
            message = f"Node type {node_type} ends in 'ies' and singular form ends in 'ie'"
            singular_value = node_type[:-1]
        else:
            message = f"Node type {node_type} ends in 'ies' and singular form ends in 'y'"
            singular_value = node_type[:-3] + 'y'
    else:
        message = f"Node type {node_type} ends in 's' and removing 's'"
        singular_value = node_type[:-1]

    if node_type != current_node_type:

        logger.info('"' * len(f'"""""""" {node_type.upper()}  """""""""""""'))
        logger.info(f'"""""""" {node_type.upper()}  """""""""""""') 
        logger.info('"' * len(f'"""""""" {node_type.upper()}  """""""""""""'))
        logger.info(message)
        current_node_type = node_type

    return singular_value


def get_fields_with_unique_patterns_in_dictionary(node_field: str) -> tuple[str, str]:
    """
    the fields that are manually added to subject and person
    in the transform script need to be manually added in the map
    """
    if node_field == "subject_submitter_id":
        return ("subject", "submitter_id")
    
    elif node_field == "timing_id":
        return ("timing", "submitter_id")

    elif node_field == "timing_type":
        return ("timing", "type")
    
    elif node_field == "person_id":
        return ("person", "submitter_id")


def map_es_field_to_dd(node: str, field: str) -> tuple[str, str]:
    """
    1) check if field is part of unique list
    2) check if field is part of subject or person
       a) set node to subject if field is in subject fields
       b) set node to person if field is in person fields
    3) if field is not part of subject or person singularize the node
    4) check if field is in _node_id format
    5) check if field is in timing fields
    6) set pointer to (node, field)
    7) validate pointer exists in data dictionary
    8) return pointer if yes otherwise return None
    """
    pointer: tuple[str, str]

    if not node:

        if field in subject_fields:

            node = "subject"

        
        elif field in person_fields:
            
            node = "person"

    if field in fields_with_unique_patterns:

        logger.info(f"{field} is part of the unique pattern list")
        pointer = get_fields_with_unique_patterns_in_dictionary(field)


    elif re.match(r"^_.*_id$", field):

        pointer = ("_".join(field.split("_")[1:-1]), "id")

    elif field in timing_fields:

        pointer = ('timing', field)

    else:

        pointer = (node, field)

    try: 
        dd_mapping[pointer[0]]["properties"][pointer[1]]
    except KeyError:
        logger.error(f"Pointer {pointer} does not exist in data dictionary")
        pointer = None

    return pointer
        



def find_es_field_in_dd(key: tuple[str, str]) -> dict[str, any]:
    """
    1) check if pointer exists
    2) check if enum in field_properties
    3) check if type in field_properties
    4) return {} no valid selectable values
    """
    if not key:
        return {}

    field_properties = dd_mapping[key[0]]["properties"].get(key[1])

    if "enum" in field_properties:

        return {"enum": field_properties["enum"]}
    
    elif "type" in field_properties:

        return {"type": field_properties["type"]}

    else:

        return {}



def create_es_dd_mapping() -> dict[str, any]:
    """
    1) iterate through list of es values returned from guppy
    2) map es value to value in data dictionary if one exists
    3) return the valid selectable values from the data dictionary
    4) add to es_dd_map
    """
    es_dd_map = {}


    for es_field in es_mapping:

        es_dd_map[es_field] = {}
        
        if "." in es_field:

            node = make_singular(es_field.split(".")[0])
            field = es_field.split(".")[1]

        
        else:

            node = None
            field = es_field

    
        logger.info(f"es field: {es_field.upper()}")

        es_dd_map[es_field]["pointer"] = map_es_field_to_dd(node, field)

        es_dd_map[es_field].update(find_es_field_in_dd(es_dd_map[es_field]["pointer"]))

        logger.info(f"{es_field} -> {es_dd_map[es_field]}")

    return es_dd_map


def add_manual_fields(es_dd_map: dict[str, any]) -> dict[str, any]:
    for key, value in config.items():
        es_dd_map[key] = value
    return es_dd_map



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for handling mapping creation.")
    
    # Add argument for 'add-manual-values'
    parser.add_argument('action', nargs='?', default='', help="Specify 'add-manual-fields' to trigger manual values method")

    # Parse the arguments
    args = parser.parse_args()

    # Run the mapping creation
    mapping = create_es_dd_mapping()

    mapping = create_es_dd_mapping()
    if args.action == 'add-manual-fields':
        mapping = add_manual_fields(mapping)  # Trigger the additional method if the argument is passed
    with open(OUTPUT_FILE, "w") as f:
        json.dump(mapping, f, indent=4)
    
    



