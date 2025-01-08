import gen3
import csv
import requests
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission
# from load import adapt_and_load



# base_url = "http://localhost"
# base_url = "https://portal-dev.pedscommons.org"
# base_url = "https://portal-demo.pedscommons.org"
base_url = "https://portal-staging.pedscommons.org"
# base_url = "https://portal.pedscommons.org"


# def load(subset=None):
# 	auth = Gen3Auth(base_url, refresh_file="../credentials.json")
# 	sub = Gen3Submission(base_url, auth)

# 	for type in types:
# 		print(type)
# 		adapt_and_load(type, sub, file_url, template_url, token, subset)


def pull_records(projects, types):
	auth = Gen3Auth(base_url, refresh_file="../credentials.json")
	sub = Gen3Submission(base_url, auth)

	data = {}
	for project in projects:
		prog,proj = project.split("-")
		data[project] = {}
		for type in types:
			print("extracting " + type + " ...")
			tmp = sub.export_node(prog, proj, type, "json")
			data[project][type] = sub.export_node(prog, proj, type, "json")["data"]
		# print(data[project]["timing"][0])
		# print(data[project]["subject"])
		# exit()
	print("fine extract")
	return data

def get_ids(data, projects, types):
	result = {}
	for project in projects:
		if project not in result:
			result[project] = {}
		for type in types:
			if type not in result[project]:
				result[project][type] =[]
			for record in data[project][type]:
				result[project][type].append(record["submitter_id"])

	return result
	

def load_missing_file_data(ids, types):
	missing_ids = []
	submitter_ids = []

	# idx = 0
	for type in types:
		tsvfile = open('../Submission_INRG_20220201/gen3_' + type + '.tsv')
		reader = csv.DictReader(tsvfile, dialect='excel-tab')
		for row in reader:
			# idx = idx + 1
			submitter_id = row["*submitter_id"]
			project_id = row["project_id"]
			type_name = row["type"]

			# if submitter_id is None or submitter_id == "":
			# 	print(row)

			# if submitter_id in submitter_ids:
			# 	print('following is duplicate:')
			# 	print(row)
			# submitter_ids.append(submitter_id)

			if submitter_id not in ids[project_id][type_name]:
				if submitter_id in missing_ids:
					print("duplicated ID: " + submitter_id)
				else:
					missing_ids.append(submitter_id)


	print(missing_ids)
	# print(idx)
	# print(len(ids["pcdc-20220201"]["secondary_malignant_neoplasm"]))
	# print(len(submitter_ids))


projects = ["pcdc-20220201"]
types = ["tumor_assessment", "secondary_malignant_neoplasm"]
data = pull_records(projects, types)
ids = get_ids(data, projects, types)
load_missing_file_data(ids, types)
# print(data)
# print(ids)

# load()
	