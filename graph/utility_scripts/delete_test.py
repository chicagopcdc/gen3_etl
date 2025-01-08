import sys
import gen3
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
from load_tom_etl import adapt_and_load
from generate import by_type


# types = ["program", "project", "person", "subject", "study", "clinical_event", "molecular_analysis", "tumor_assessment", "survival_characteristic", "staging", "histology", "biopsy_surgical_proc$
# types = ["program", "project", "person"]
# types = ["subject"]
# types = ["study"]
# types = ["clinical_event"]
# types = ["molecular_analysis"]
# types = ["tumor_assessment"]
# types = ["survival_characteristic"] # missing mapping for Uknown
# types = ["staging"]
# types = ["histology"]
# types = ["biopsy_surgical_procedure"]

def delete():
        endpoint = "http://localhost"
        # endpoint = "https://portal.pedscommons.org"
        auth = Gen3Auth(endpoint, refresh_file="credentials.json")
        sub = Gen3Submission(endpoint, auth)
        response = sub.delete_record("pcdc", "2020_11_20", "2798f08a-761f-436c-bd5f-f867ef2afb84")
        print(response)

	
        # for type in types:
        #        print(type)
        #        adapt_and_load(type, sub)


print(sys.argv)
if len(sys.argv) > 1:
        if sys.argv[1] == "delete":
                delete()
        elif sys.argv[1] == "load":
                delete()
