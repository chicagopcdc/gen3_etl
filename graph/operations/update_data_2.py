# This is intended to be used when the data dictionary has changed and some values need to be updated.

import re
import requests
from deepdiff import DeepDiff
import json
# import requests
# import json
import csv


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
	return diff.to_json()
	# return diff.to_dict()

# TODO need to start versioning the template with the dictionary in pcdcdictionaries
def get_differences(old_sub, new_sub):
	# Load dictioanries Extract important info
	sum_old = summarize_dd(old_sub)
	sum_new = summarize_dd(new_sub)

	# Compare dictionaries
	diff_sum = dict_compare(sum_old, sum_new)
	diff_sum_obj = json.loads(diff_sum)

	# Save to file
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
		if k == "enum" or k == "type":
			if k not in latest:
				latest[k] = {}
			latest[k][old_value] = new_value
			# latest[k].append((old_value,new_value))
			break

		if k not in latest:
			latest[k] = {}
		latest = latest[k]

		if k == keys[len(keys) - 1]:
			latest[old_value] = new_value
		
	return d
		
def convert_dict(keys_string, dict):
	keys_string = keys_string.replace('root','')
	keys_string = keys_string.replace('[','')
	keys_string = keys_string.replace("'",'')
	keys = keys_string.split(']')[:-1]
	
	d = dict if dict else {}
	latest = d
	for k in keys:
		if k == "enum" or k == "type":
			if k not in latest:
				latest[k] = {}
			latest[k][old_value] = new_value
			# latest[k].append((old_value,new_value))
			break

		if k not in latest:
			latest[k] = {}
		latest = latest[k]

		if k == keys[len(keys) - 1]:
			latest[old_value] = new_value
		
	return d


def query_ids(sub, node, attribute, old_v):
	items = []
	page_size = 50
	offset = 0
	while True:
		query = '{ ' + node + '(' + attribute + ': ' + json.dumps(old_v) + ', first:' + json.dumps(page_size) + ', offset:' + json.dumps(offset) + '){ submitter_id, id, project_id } }'
		ret = sub.query(query)
		num_data = len(ret['data'][node])
		items.extend([item for item in ret['data'][node]])
		if num_data >= page_size:
			offset += page_size
		else:
			return items

def update_record(sub, item, attribute, old_v, new_v):
	program_name_tmp,project_code_tmp = item['project_id'].split('-')

	# Retrieve the node
	ret = sub.export_record(program_name_tmp, project_code_tmp, item['id'], "json")
	if len(ret) == 1:
		ret = ret[0]

		# Edit record
		del ret["id"]
		# del record["project_id"] ??
		if new_v:
			ret[attribute] = new_value
		else:
			del ret[attribute]

		# # Delete old record since updating a record overwrite or add new items but doesn't delete old values
		# res = old_sub.delete_record(program_name_tmp, project_code_tmp, item['id'])
		# # Add updated record with the same subject and clinical even connections and same submitter_id
		# res = old_sub.submit_record(program_name_tmp, project_code_tmp, ret)
		# Delete record
		res = sub.delete_record(program_name_tmp, project_code_tmp, items['id'])
		
		# Re-add record
		try:
			res = sub.submit_record(program_name_tmp, project_code_tmp, ret)
		except requests.HTTPError as exception:
			print(exception)
			print(exception.response.status_code)
			print(exception.response.content)
			print(record)	
			failed_update.append(record["submitter_id"])
	else:
		# It should nevet get here since this node id are retrieved from a query and should always exists
		if len(ret) == 0:
			print("ERROR: ENTITY NOT FOUND!")
		else:
			print("ERROR: ENTITY NOT FOUND OR TOO MANY MATCHED!!!")
			print(len(ret))
			print(ret)
			error_nodes.extend(ret)

def load_differences(old_sub):
	with open('old_summary.json') as old_summary, open('new_summary.json') as new_summary, open('diff_summary.json') as diff_summary:
		sum_old = json.load(old_summary)
		sum_new = json.load(new_summary)
		sum_diff = json.load(diff_summary)

		
		### DEAL WITH ATTRIBUTES 
		dict_removed = {}
		dict_added = {}

		if "dictionary_item_added" in sum_diff:
			print("WARNING: The following items have been added to the dictionary, please make sure all the new data will be loaded.")
			print(sum_diff["dictionary_item_added"])

			for value in sum_diff["dictionary_item_added"]:
				dict_added = convert(key, value, None, dict_added)

			if proceed():
				# for added in dictionary_item_added:
				# 	# nothing to do if there are no data to add from the old dataset
				# 	# otherwise run script to gether external connections / new data from a source to update data
				# 	break
				print("WARNING: This part hasn't been supported yet. Look into the code for implementation")
			else:
				print("`dictionary_item_added` has been skipped.")

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



		#### DEAL WITH VALUES
		# Handle value change, addition, removal
		value_removed = {}
		value_added = {}
		value_changed = {}
		value_changed_type = {}
		value_changed_description = {}

		# Added values
		if "iterable_item_added" in sum_diff:
			# Load new values
			for key,value in sum_diff["iterable_item_added"].items():
				value_added = convert(key, value, None, value_added)

		# Removed values
		if "iterable_item_removed" in sum_diff:
			# Load removed values
			for key,value in sum_diff["iterable_item_removed"].items():
				value_removed = convert(key, value, None, value_removed)


		# Changed values or comparison misunderstandings (value updated because in the same place as another, but really are both existing value)
		if "values_changed" in sum_diff:
			for key,value in sum_diff["values_changed"].items():
				value_changed = convert(key, value["old_value"], value["new_value"], value_changed)

			# Divide this in old_value -> value_removed and new_value -> value_added when it is an enum, maintain as value changed the direct comparison, like description for instance 
			for node,attributes in value_changed.items():
				for attribute,changed in attributes.items():
					for key,values in changed.items():
						if key == "enum":
							for old_v,new_v in values.items():
								# If old_v in removed otherwise add there
								if node not in value_removed:
									value_removed[node] = {}
								if attribute not in value_removed[node]:
									value_removed[node][attribute] = {}
								if key not in value_removed[node][attribute]:
									value_removed[node][attribute][key] = {}
								if old_v not in value_removed[node][attribute][key]:
									value_removed[node][attribute][key][old_v] = None

								# If new_v in added, otherwise add there
								if node not in value_added:
									value_added[node] = {}
								if attribute not in value_added[node]:
									value_added[node][attribute] = {}
								if key not in value_added[node][attribute]:
									value_added[node][attribute][key] = {}
								if new_v not in value_added[node][attribute][key]:
									value_added[node][attribute][key][new_v] = None
						elif key == "type":
							for old_v,new_v in values.items():
								if node not in value_changed_type:
									value_changed_type[node] = {}
								if attribute not in value_changed_type[node]:
									value_changed_type[node][attribute] = {}
								if key not in value_changed_type[node][attribute]:
									value_changed_type[node][attribute][key] = {}
								if old_v not in value_changed_type[node][attribute][key]:
									value_changed_type[node][attribute][key][old_v] = new_v
						elif key == "description":
							for old_v,new_v in values.items():
								if node not in value_changed_description:
									value_changed_description[node] = {}
								if attribute not in value_changed_description[node]:
									value_changed_description[node][attribute] = {}
								if key not in value_changed_description[node][attribute]:
									value_changed_description[node][attribute][key] = {}
								if old_v not in value_changed_description[node][attribute][key]:
									value_changed_description[node][attribute][key][old_v] = new_v
						else:
							print("ERROR: Type not recognized: " + str(key))

			# Intersect removed with added to make sure some of the one categorized as removed are not also added and clena it up
			for node,attributes in value_removed.items():
				if node in value_added:
					for attribute,changed in attributes.items():
						if attribute in value_added[node]:
							for key,values in changed.items():
								if key == "enum" and key in value_added[node][attribute]:
									for old_v,new_v in values.items():
										if old_v in value_added[node][attribute][key]:
											# Item in removed list is present also in the added list, cleanup
											del value_added[node][attribute][key][old_v]
											del value_removed[node][attribute][key][old_v]


		# TODO deal with description (no need for anything really) and type (should just be a typo check) separately, this is just for `enum`
		### Now we have all the enum removed/updated in one place (value_removed)
		# Check which one as actual data to reduce the amount of questions asked to the user
		node_to_map = []
		summary = []
		for node,attributes in value_removed.items():
			for attribute,changed in attributes.items():
				for key,values in changed.items():
					if key == "enum":
						for old_v,new_v in values.items():
							# Query the records identifiers for this item
							items = query_ids(old_sub,node,attribute,old_v)

							# Skip the deleted items without record in the DB
							if len(items) > 0:
							# if len(items) == 0:
								# del value_removed[node][attribute][key][old_v]
							# else:
								tmp = {} 
								tmp[node] = {}
								tmp[node][attribute] = {}
								tmp[node][attribute][old_v] = {}
								tmp[node][attribute][old_v]["proposed_value"] = new_v

								summary.append(tmp.copy())

								tmp[node][attribute][old_v]["items"] = items

								node_to_map.append(tmp)

		# print(node_to_map)
		# Save full summary 
		with open('mapping_summary.json', 'w') as review_summary:
			json.dump(node_to_map, review_summary)
		
		# Generates short summary
		rows = []
		for item in node_to_map:
			for node,attributes in item.items():
				for attribute,values in attributes.items():
					for key,value in values.items():
						# del value["items"]
						rows.append({"node": node, "attribute": attribute, "value_to_be_cancelled": key, "proposed_value": value["proposed_value"]})

		# Save short summary to file
		with open('review_mapping.csv', 'w') as tsvfile:
			mapping_file = csv.DictWriter(tsvfile, fieldnames=["node", "attribute", "value_to_be_cancelled", "proposed_value"], dialect='excel-tab')
			mapping_file.writeheader()
			mapping_file.writerows(rows)


		# Save the type and description changes
		with open('type_summary.json', 'w') as type_summary:
			json.dump(value_changed_type, type_summary)
		with open('description_summary.json', 'w') as desc_summary:
			json.dump(value_changed_description, desc_summary)




		# Load mapping with updated translation
		with open('mapping_summary.json') as review_summary:
			node_to_map = json.load(review_summary)
			print(node_to_map)


		
			final_nodes = []
			error_nodes = []
			for item in node_to_map:
				for node,attributes in item.items():
					for attribute,values in attributes.items():
						for key,value in values.items():
							items = value["items"]

							for item in items:
								update_record(old_sub, item, attribute, key, value["proposed_value"]):





	