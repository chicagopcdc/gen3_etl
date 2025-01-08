# This is intended to be used when the data dictionary has changed and some values need to be updated.

import re
import requests
from deepdiff import DeepDiff
# import deepdiff
import json
# import requests
# import json
# import csv


def proceed():
	answer = None 
	while answer not in ("yes", "no"): 
		answer = input("Do you want to proceed? Enter yes or no: ") 
		if answer == "yes": 
			return True 
		elif answer == "no": 
			return False 
		else: 
			print("Please enter yes or no.")


# def dict_compare(d1, d2):
#     d1_keys = set(d1.keys())
#     d2_keys = set(d2.keys())
#     shared_keys = d1_keys.intersection(d2_keys)
#     added = d1_keys - d2_keys
#     removed = d2_keys - d1_keys
#     modified = {o : (d1[o], d2[o]) for o in shared_keys if d1[o] != d2[o]}
#     same = set(o for o in shared_keys if d1[o] == d2[o])
#     return added, removed, modified, same

# x = dict(a=1, b=2)
# y = dict(a=2, b=2)
# added, removed, modified, same = dict_compare(x, y)

def summarize_dd(sub):
	"""Return a dict with nodes and list of properties in each node."""
	dd = sub.get_dictionary_all()
	nodes = []
	node_regex = re.compile(
		r"^[^_][A-Za-z0-9_]+$"
	)  # don't match _terms,_settings,_definitions, etc.)
	nodes = list(filter(node_regex.search, list(dd)))

	# dds = {}
	# for node in nodes:
	#     dds[node] = []
	#     props = list(dd[node]["properties"])
	#     for prop in props:
	#         dds[node].append(prop)

	# return dds

	dds = {}
	for node in nodes:
		dds[node] = {}
		for key, value in dd[node]["properties"].items():
			dds[node][key] = {}
			for item_name, item_value in value.items():
				dds[node][key][item_name] = item_value

	return dds


class SetEncoder(json.JSONEncoder):
	def default(self, obj):
		if isinstance(obj, set):
			return list(obj)
		# if isinstance(obj, PrettyOrderedSet):
		# 	return list(obj)
		return json.JSONEncoder.default(self, obj)


def dict_compare(dict_1, dict_2):
	diff = DeepDiff(dict_1, dict_2, ignore_order=True)
	# print("LUCA SUMMARY")

	# def set_default(obj):
	# 	if isinstance(obj, set):
	# 		return list(obj)
	# 	raise TypeError

	# try:
	return diff.to_json()
	# return diff.to_dict()
	# print(json.dumps(diff, cls=SetEncoder, indent=4))
	# print(json.dumps(diff, default=set_default, indent=4))
	# print(json.dumps(json.loads(diff.to_json()), indent=4))  
	# except:


# TODO need to start versioning the template with the dictionary in pcdcdictionaries
def get_differences(old_sub, new_sub):

	# Load dictioanries
	# dd_old = old_sub.get_dictionary_all()
	# dd_new = new_sub.get_dictionary_all()

	# Extract important info
	sum_old = summarize_dd(old_sub)
	sum_new = summarize_dd(new_sub)
	# print("OLD LUCA 1")
	# print(json.dumps(sum_old))
	# print("NEW LUCA 1")
	# print(json.dumps(sum_new))

	# Compare dictionaries
	diff_sum = dict_compare(sum_old, sum_new)
	diff_sum_obj = json.loads(diff_sum)

	# Save to file
	# with open('data.json', 'w', encoding='utf-8') as f:
 #    json.dump(data, f, ensure_ascii=False, indent=4)
	with open('old_summary.json', 'w') as old_summary, open('new_summary.json', 'w') as new_summary, open('diff_summary.json', 'w') as diff_summary:
		json.dump(sum_old, old_summary)
		json.dump(sum_new, new_summary)
		json.dump(diff_sum_obj, diff_summary)


def convert(keys_string, old_value, new_value, dict):
	keys_string = keys_string.replace('root','')
	keys_string = keys_string.replace('[','')
	keys_string = keys_string.replace("'",'')
	keys = keys_string.split(']')[:-1]
	
	d = dict if dict else {}
	latest = d
	for k in keys:
		if k == "enum":
			latest[old_value] = new_value
			break

		if k not in latest:
			latest[k] = {}
		latest = latest[k]

		if k == keys[len(keys) - 1]:
			latest[old_value] = new_value
		
	return d


# UPDATE THE dictioany on the environment before sunning this
def apply_changes(old_sub, value_changed):

	missing_mapping = []
	failed_update = []
	
	for updating_type,node_value in value_changed.items():
		for variable,variable_value in node_value.items():
			for old_value,new_value in variable_value.items():
				# Searching for the items with the property that needs to be updated or removed
				# SEARCH TEMPLATE
				# {
				#   _staging_count(stage: "Stage 0 (AJCC)"),
				#   staging(stage: "Stage 0 (AJCC)", first: 30000){
				#     submitter_id,
				#   }
				# }
				query = '{ ' + updating_type + '(' + variable + ': ' + json.dumps(old_value) + ', first: 30000) { id, project_id } }'
				ret = old_sub.query(query)
				ret = ret["data"][updating_type]
				# print(variable)
				# print(old_value)
				# print (len(ret))

				if len(ret) > 0 and new_value is None:
					print("ERROR: Review " + updating_type + " " + variable + " " + old_value + ". Has been deleted in the new dictionary and we have no automatic new mapping for it.")
					missing_mapping.push(str(updating_type + " " + variable + " " + old_value))
					continue
				else:
					for item in ret:
						[program_name, project_code] = item["project_id"].split('-')
						records = old_sub.export_record(program_name, project_code, item["id"], "json")
						for record in records:
							# print(record)
							record[variable] = new_value
							del record["project_id"]
							# print(record)

							try:
								return_value = old_sub.submit_record(program_name, project_code, record)
							except requests.HTTPError as exception:
								print(exception)
								print(exception.response.status_code)
								print(exception.response.content)
								print(record)	
								failed_update.append(record["submitter_id"])		
								
				
	print("ERROR: Records without mapping:")
	print(missing_mapping)
	print("ERROR: Records with failed update:")
	print(failed_update)
		

def load_differences(old_sub):
	with open('old_summary.json') as old_summary, open('new_summary.json') as new_summary, open('diff_summary.json') as diff_summary:
		sum_old = json.load(old_summary)
		sum_new = json.load(new_summary)
		sum_diff = json.load(diff_summary)

		value_removed = {}
		value_added = {}
		

		if "dictionary_item_added" in sum_diff:
			print("WARNING: The following items have been added to the dictionary, please make sure all the new data will be loaded.")
			print(sum_diff["dictionary_item_added"])

			if proceed():
				# for added in dictionary_item_added:
				# 	# nothing to do if there are no data to add from the old dataset
				# 	# otherwise run script to gether external connections / new data from a source to update data
				# 	break
				print("WARNING: This part hasn't been supported yet. Look into the code for implementation")
			else:
				print("`dictionary_item_added` has been skipped.")


		if "iterable_item_added" in sum_diff:
			for key,value in sum_diff["iterable_item_added"].items():
				value_added = convert(key, value, None, value_added)

			print("WARNING: The following items have been added to the enum in the dictionary, please make sure all the new data will be loaded.")
			print(sum_diff["iterable_item_added"])

			# if proceed():
			# 	# for item in iterable_item_added:
			# 	# 	# nothing to do since there are no old value that needs to be updated
			# 	# 	break
			# 	print("WARNING: This part hasn't been supported yet. Look into the code for implementation")
			# else:
			# 	print("`iterable_item_added` has been skipped.")


		if "dictionary_item_removed" in sum_diff:
			print("WARNING: This is a breaking change. A person attention is needed to make sure ")
			print(sum_diff["dictionary_item_removed"])
			# exit()
			if proceed():
				# for removed in sum_diff["dictionary_item_removed"]:
				# 	#TODO run a search to see if there is any item in the node deletede or with the property deleted
				# 	# Big deal if a node is removed - should be handled manually
				# 	# if approved the nodes should be removed before making this update
				# 	break
				print("WARNING: This part hasn't been supported yet. Look into the code for implementation")
			else:
				print("`dictionary_item_removed` has been skipped.")
			

		if "iterable_item_removed" in sum_diff:
			# Build a computable list of the values
			for key,value in sum_diff["iterable_item_removed"].items():
				value_removed = convert(key, value, None, value_removed)
			# print(value_removed)

			final_nodes = []
			error_nodes = []
			undecided_nodes = []
			# For each deleted values ignore it no entity is using it, otherwise try to assign the updated ones. It could also be the value is correct and was removed from the DD by mistake.
			for node,attributes in value_removed.items():
				for attribute,values in attributes.items():
					if attribute != "submitter_id":
						for old_v,new_v in values.items():
							items = []

							# Retrieve all the nodes with the old_v to be removed
							page_size = 50
							offset = 0
							while True:
								query = '{ ' + node + '(' + attribute + ': ' + json.dumps(old_v) + ', first:' + json.dumps(page_size) + ', offset:' + json.dumps(offset) + '){ submitter_id, id, project_id } }'
								ret = old_sub.query(query)
								num_data = len(ret['data'][node])
								items.extend([item for item in ret['data'][node]])
								if num_data >= page_size:
									offset += page_size
								else:
									break

							
							for item in items:
								program_name_tmp,project_code_tmp = item['project_id'].split('-')

								# Retrieve the node
								ret = old_sub.export_record(program_name_tmp, project_code_tmp, item['id'], "json")
								if len(ret) == 1:
									ret = ret[0]

									# if new_v is None:
									# 	del ret[attribute]
									# New values added for that attribute
									new_attribute_value_tmp = [key for key,value in value_added[node][attribute].items()]
									print("The value: " + old_v + " for the attribute: " + attribute + " in the node: " + node + " won't exist anymore.")
									print("You can check the dictionary page on the portal for a full list or the following are new values added. The new value may be just different because of the spelling. Please check among the following and type the one that will update this value. If none of them is correct you can skip and evaluate later pressing enter without typing anything else.")
									print(new_attribute_value_tmp)
									answer = input("Which one is the new value?") 

									if answer in new_attribute_value_tmp:
										# Assign a new value to this node
										ret[attribute] = answer
										final_nodes.append(ret)
									else:
										# Put this aside and think about them later
										undecided_nodes.append(ret)
								else:
									# It should nevet get here since this node id are retrieved from a query and should always exists
									if len(ret) == 0:
										print("ERROR: ENTITY NOT FOUND!")
									else:
										print("ERROR: ENTITY NOT FOUND OR TOO MANY MATCHED!!!")
										print(len(ret))
										print(ret)
										error_nodes.extend(ret)



			# del ret['id']

			# # Delete old record since updating a record overwrite or add new items but doesn't delete old values
			# res = old_sub.delete_record(program_name_tmp, project_code_tmp, item['id'])
			# # Add updated record with the same subject and clinical even connections and same submitter_id
			# res = old_sub.submit_record(program_name_tmp, project_code_tmp, ret)

			##TODO REMOVE - JUST FOR THIS FIRST CHANGE WE NEED MANUAL INTERACTION
			# del value_changed["subject"]
			##END REMOVE


			# print("WARNING: Make sure the item removed are not associated to any entity.")
			# print(sum_diff["iterable_item_removed"])

			if proceed():
				apply_changes(old_sub, value_changed)
			else:
				print("`iterable_item_removed` has been skipped.")

			
		# if value has changed retrieve the old one, update the JSON and resubmit the entity with the updated value.
		if "values_changed" in sum_diff:
			# test = {}
			# test["root['molecular_analysis']['dna_index']['enum'][0]"] = { "new_value": "DNA Index <= 1 (Hypodiploid, Diploid)", "old_value": "DNA Index </= 1 (Hypodiploid, Diploid)"}
			# test["root['subject']['consortium']['enum'][1]"] = { "new_value": "INSTRuCT", "old_value": "Germ Cell Tumors"}

			value_changed = {}
			for key,value in sum_diff["values_changed"].items():
				value_changed = convert(key, value["old_value"], value["new_value"], value_changed)
				# print(value_changed)


			##TODO REMOVE - JUST FOR THIS FIRST CHANGE WE NEED MANUAL INTERACTION
			del value_changed["subject"]
			##END REMOVE
	
			# print(value_changed)
			# value_changed = {}
			# value_changed["staging"] = {}
			# value_changed["staging"]["stage"] = {}
			# value_changed["staging"]["stage"]["Stage 0 (AJCC)"] = "Stage 0"
			# value_changed["staging"]["stage"]["Stage I (FIGO)"] = "Stage I"
			# value_changed["staging"]["stage"]["Stage I (AJCC)"] = "Stage I"
			# value_changed["staging"]["stage"]["Stage I (COG)"] = "Stage I"
			# value_changed["staging"]["stage"]["Stage IA (AJCC)"] = "Stage IA"
			# value_changed["staging"]["stage"]["Stage IB (AJCC)"] = "Stage IB"
			# value_changed["staging"]["stage"]["Stage IS (AJCC)"] = "Stage IS"
			# value_changed["staging"]["stage"]["Stage II (COG)"] = "Stage II"
			# value_changed["staging"]["stage"]["Stage II (FIGO)"] = "Stage II"
			# value_changed["staging"]["stage"]["Stage II (AJCC)"] = "Stage II"
			# value_changed["staging"]["stage"]["Stage IIA (AJCC)"] = "Stage IIA"
			# value_changed["staging"]["stage"]["Stage IIB (AJCC)"] = "Stage IIB"
			# value_changed["staging"]["stage"]["Stage IIC (AJCC)"] = "Stage IIC"
			# value_changed["staging"]["stage"]["Stage III (FIGO)"] = "Stage III"
			# value_changed["staging"]["stage"]["Stage III (COG)"] = "Stage III"
			# value_changed["staging"]["stage"]["Stage III (AJCC)"] = "Stage III"
			# value_changed["staging"]["stage"]["Stage IIIA (AJCC)"] = "Stage IIIA"
			# value_changed["staging"]["stage"]["Stage IIIB (AJCC)"] = "Stage IIIB"
			# value_changed["staging"]["stage"]["Stage IIIC (AJCC)"] = "Stage IIIC"
			# value_changed["staging"]["stage"]["Stage IV (COG)"] = "Stage IV"
			# value_changed["staging"]["stage"]["Stage IV (FIGO)"] = "Stage IV"
			# value_changed["staging"]["stage"]["Stage IVs (COG)"] = "Stage IVs"
			# value_changed["molecular_analysis"] = {}
			# value_changed["molecular_analysis"]["dna_index"] = {}
			# value_changed["molecular_analysis"]["dna_index"]["DNA Index </= 1 (Hypodiploid, Diploid)"] = "DNA Index <= 1 (Hypodiploid, Diploid)"
			# value_changed["tumor_assessment"] = {}
			# value_changed["tumor_assessment"]["tumor_site"] = {}
			# value_changed["tumor_assessment"]["tumor_site"]["Distant lymph nodes"] = "Distant Lymph Nodes"

			if proceed():
				apply_changes(old_sub, value_changed)
			else:
				print("`values_changed` has been skipped.")
			

		
			
			


	


