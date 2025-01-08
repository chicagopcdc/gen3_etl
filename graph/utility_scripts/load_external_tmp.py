import gen3
import json
import requests
from gen3.auth import Gen3Auth
from gen3.submission import Gen3Submission


endpoint = "http://localhost"
auth = Gen3Auth(endpoint, refresh_file="credentials.json")
sub = Gen3Submission(endpoint, auth)

with open('../fake_data/external_fake/external_reference.json') as template_file:
	# load template
	template_obj = json.load(template_file)
	
	for entity in template_obj:
		try:
			response = sub.submit_record("pcdc", "20210325", entity)
		except requests.HTTPError as exception:
			print(exception)
			print(exception.response.status_code)
			print(exception.response.content)
			print(entity)
