from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission

# INSTRuCT
types = ["biopsy_surgical_procedure", "secondary_malignant_neoplasm", "histology", "staging", "survival_characteristic", "tumor_assessment", "molecular_analysis", "study", "subject", "person"]
program_name = "pcdc"
project_code = "20211006"
base_url = "https://portal.pedscommons.org"

auth = Gen3Auth(base_url, refresh_file="../credentials.json")
sub = Gen3Submission(base_url, auth)
sub.delete_nodes(program_name, project_code, types)
sub.delete_project(program_name, project_code)
