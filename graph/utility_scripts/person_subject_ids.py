import requests
import json
import csv
import sys
import os

def transform(person_file_path, subject_file_path, person_new_file_path, subject_new_file_path):
	with open(person_file_path) as person_file, open(subject_file_path) as subject_file, open(person_new_file_path, 'w') as person_file_w, open(subject_new_file_path, 'w') as subject_file_w:
		person_reader = csv.DictReader(person_file, dialect='excel-tab')
		# csv.reader(tsv_file, delimiter="\t")
		subject_reader = csv.DictReader(subject_file, dialect='excel-tab')
	
		people = []
		subjects = []
		p_to_s = {}

		idx = 256789
		for person in person_reader:
			p_to_s[person["*submitter_id"]] = "person_" + str(idx)
			person["*submitter_id"] = "person_" + str(idx)
			idx += 1
			people.append(person)

		for subject in subject_reader:
			subject["*persons.submitter_id"] = p_to_s[subject["*submitter_id"]]
			subjects.append(subject)
		

		new_person = csv.DictWriter(person_file_w, fieldnames=person_reader.fieldnames, dialect='excel-tab')
		new_person.writeheader()
		new_person.writerows(people)

		new_subject = csv.DictWriter(subject_file_w, fieldnames=subject_reader.fieldnames, dialect='excel-tab')
		new_subject.writeheader()
		new_subject.writerows(subjects)
		
		os.remove(person_file_path)
		os.remove(subject_file_path)
		os.rename(subject_new_file_path, subject_file_path)
		os.rename(person_new_file_path, person_file_path)

print(sys.argv)
if len(sys.argv) == 2:
	path_file = sys.argv[1]
	transform(path_file + "gen3_person.tsv", path_file + "gen3_subject.tsv", path_file + "gen3_person_updated.tsv", path_file + "gen3_subject_updated.tsv")
