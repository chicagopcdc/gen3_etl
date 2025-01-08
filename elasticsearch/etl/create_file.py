import json
from build_json import file_path, generate_subject_json


data = generate_subject_json(file_path)

with open("../files/es_data_staging_0523.json", "w") as out_file:
	json.dump(data, out_file)


