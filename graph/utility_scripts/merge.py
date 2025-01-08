import requests
import json
import csv


# type = "person"
# type = "subject"
# type = "clinical_event"
# type = "histology"
# type = "molecular_analysis"
# type = "secondary_malignant_neoplasm"
# type = "staging"
# type = "study"
# type = "survival_characteristic"
type = "tumor_assessment"
with open('./Gen3_2/gen3_' + type + '.tsv') as src1, open('./quick_load/gen3_' + type + '.tsv') as src2, open('./test/gen3_' + type + '.tsv', 'w') as merged_file:

		# load data to be merged
		reader1 = csv.DictReader(src1, dialect='excel-tab')
		reader2 = csv.DictReader(src2, dialect='excel-tab')

		headers1 = reader1.fieldnames.copy()
		headers1.sort()
		headers2 = reader2.fieldnames.copy()
		headers2.sort()

		merged_headers = None
		if headers1==headers2:
			merged_headers = headers1
		else:
			merged_headers = headers1 + list(set(headers2) - set(headers1))

		merged = []
		for row in reader1:
			obj_tmp = {}
			for col in headers1:
				obj_tmp[col] = row[col]
			diff = list(set(merged_headers) - set(headers1))
			for col in diff:
				obj_tmp[col] = None
			merged.append(obj_tmp)

		for row in reader2:
			obj_tmp = {}
			for col in headers2:
				obj_tmp[col] = row[col]
			diff = list(set(merged_headers) - set(headers2))
			for col in diff:
				obj_tmp[col] = None
			merged.append(obj_tmp)

		external_writer = csv.DictWriter(merged_file, fieldnames=merged_headers, dialect='excel-tab')
		external_writer.writeheader()
		external_writer.writerows(merged)


	






			
		