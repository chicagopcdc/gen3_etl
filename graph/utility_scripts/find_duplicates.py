import csv

removefile = open('INRG_COG_CASES_TO_REMOVE.csv')
remove_reader = csv.DictReader(removefile)

instructfile = open('./Submission_INSTRuCT_20220201-2/gen3_subject.tsv')
instruct_reader = csv.DictReader(instructfile, dialect='excel-tab')

inrgfile = open('./Submission_INRG_20220201-2//gen3_subject.tsv')
inrg_reader = csv.DictReader(inrgfile, dialect='excel-tab')


inrg_sub_id = []
instr_sub_id = []

for row in instruct_reader:
    if row["*submitter_id"]:
        instr_sub_id.append(row["*submitter_id"])
print(len(instr_sub_id))
instructfile.close()

for row in inrg_reader:
    if row["*submitter_id"]:
        inrg_sub_id.append(row["*submitter_id"])
print(len(inrg_sub_id))
inrgfile.close()

# both = []
# for row in remove_reader:
#     if row["USI"]:
#         # print("COG_" + row["USI"])
#         submitter_id = "COG_" + row["USI"]
#         if submitter_id in inrg_sub_id and submitter_id in instr_sub_id:
#             both.append(submitter_id)

both = []
for submitter_id in inrg_sub_id:
    if submitter_id in instr_sub_id:
        both.append(submitter_id)
both2 = []
for submitter_id in instr_sub_id:
    if submitter_id in inrg_sub_id:
        both2.append(submitter_id)

both.extend(both2)
both = list(set(both))


print(len(both))
print(both)

after_remove = []
for row in remove_reader:
    if row["USI"]:
        # print("COG_" + row["USI"])
        submitter_id = "COG_" + row["USI"]
        if submitter_id in both:
            after_remove.append(submitter_id)
removefile.close()

print(len(after_remove))
print(after_remove)
        



