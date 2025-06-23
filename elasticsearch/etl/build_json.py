# import requests
import json
import csv
import os
import io
import requests
from pathlib import Path


file_path = str(Path("../../graph/fake_data/quick_load").resolve()) + "/"
# file_path = str(Path("../../graph/Submission_all_20220201").resolve()) + '/'


# types = ["person", "subject", "study", "timing", "molecular_analysis", "tumor_assessment", "survival_characteristic", "staging", "histology", "secondary_malignant_neoplasm", "biopsy_surgical_procedure", "lab", "disease_characteristic", "external_reference"]
# types = ["person", "subject", "study", "molecular_analysis", "tumor_assessment", "survival_characteristic", "staging", "histology", "secondary_malignant_neoplasm", "biopsy_surgical_procedure", "lab", "disease_characteristic", "external_reference"]

# INSTRUCT
# types = ["person", "subject", "study", "molecular_analysis", "tumor_assessment", "survival_characteristic", "staging", "histology", "secondary_malignant_neoplasm", "biopsy_surgical_procedure"]
# INSTRUCT + INRG
# types = ["person", "subject", "study", "molecular_analysis", "tumor_assessment", "survival_characteristic", "staging", "histology", "secondary_malignant_neoplasm", "lab", "disease_characteristic", "biopsy_surgical_procedure"]


# FAKE DATA
# types = ["person", "subject", "study", "molecular_analysis", "tumor_assessment", "survival_characteristic", "staging", "histology", "secondary_malignant_neoplasm", "biopsy_surgical_procedure"]
# INRG
types = [
    "person",
    "subject",
    "study",
    "timing",
    "molecular_analysis",
    "tumor_assessment",
    "survival_characteristic",
    "staging",
    "histology",
    "secondary_malignant_neoplasm",
    "lab",
    "disease_characteristic",
    "external_reference",
]  # "program", "project", , "external_reference"]
# types = ["person", "subject", "timing", "lab", "disease_characteristic"]
number_key = [
    "age_at",
    "age_at_vitals",
    "vitals_result_numeric",
    "age_at_total_dose_start",
    "age_at_total_dose_end",
    "total_dose_administered",
    "age_at_ae",
    "age_at_medication_start",
    "age_at_medication_end",
    "age_at_gts",
    "age_at_ihc",
    "ihc_result_numeric",
    "age_at_rt_start",
    "age_at_rt_end",
    "rt_dose",
    "num_fraction",
    "age_at_sct",
    "age_at_response",
    "sct_cycles",
    "age_at_censor_status",
    "dna_index_numeric",
]
array_key = ["cause_of_death"]


def toNum(s):
    try:
        return int(s)
    except ValueError:
        return float(s)


def toArray(s):
    try:
        values = s.split(";; ")
        return values
    except error:
        print("Error transforming in array value: " + s)


def get_subject_by_id(id, type, list):
    # 1) expectedResult = [d for d in exampleSet if d['type'] in keyValList]
    # 2) list(filter(lambda d: d['type'] in keyValList, exampleSet))

    ret = [d for d in list if d[type] == id]

    num_result = len(ret)
    if num_result == 0:
        print("ERROR - Subject Not Found")
        return None
    elif num_result == 1:
        return ret[0]
    else:
        print("ERROR - Too many results for Subject")
        return None


def load_timings(file_path):
    # Load from local file
    tsvfile = open(file_path + "gen3_timing.tsv")

    reader = csv.DictReader(tsvfile, dialect="excel-tab")

    timings = []

    for row in reader:
        timing = {}
        if row["*subjects.submitter_id"]:
            timing["subject_submitter_id"] = row["*subjects.submitter_id"]
        if row["*submitter_id"]:
            timing["timing_id"] = row["*submitter_id"]

        if "age_at_course_start" in row and row["age_at_course_start"]:
            timing["age_at_course_start"] = toNum(row["age_at_course_start"])
        if "age_at_course_end" in row and row["age_at_course_end"]:
            timing["age_at_course_end"] = toNum(row["age_at_course_end"])
        if row["age_at_disease_phase"]:
            timing["age_at_disease_phase"] = toNum(row["age_at_disease_phase"])
        if "course_number" in row and row["course_number"]:
            timing["course_number"] = toNum(row["course_number"])
        if row["disease_phase_number"]:
            timing["disease_phase_number"] = toNum(row["disease_phase_number"])
        if row["year_at_disease_phase"]:
            timing["year_at_disease_phase"] = toNum(row["year_at_disease_phase"])

        timing["timing_type"] = row["timing_type"]
        if "course" in row:
            timing["course"] = row["course"]
        timing["disease_phase"] = row["disease_phase"]

        timings.append(timing)

    return timings


def get_timing_by_ids(subject_id, timing_id, list):
    ret = [
        d
        for d in list
        if "subject_submitter_id" in d
        and "timing_id" in d
        and d["subject_submitter_id"] == subject_id
        and d["timing_id"] == timing_id
    ]
    # ret = [d for d in list if "timing_id" in d and d["subject_submitter_id"] == subject_id and d["timing_id"] == timing_id]

    num_result = len(ret)
    if num_result == 1:
        return ret[0]
    elif num_result > 1:
        print("ERROR - Too many results")
    else:
        if timing_id:
            print("ERROR - Clinical Event Not Found")
            print(timing_id)
            print(subject_id)
        return None


def flatten_timing(main_dict, event):
    if "age_at_course_end" in event and event["age_at_course_end"]:
        main_dict["age_at_course_end"] = event["age_at_course_end"]
    if "age_at_course_start" in event and event["age_at_course_start"]:
        main_dict["age_at_course_start"] = event["age_at_course_start"]
    if "age_at_disease_phase" in event and event["age_at_disease_phase"]:
        main_dict["age_at_disease_phase"] = event["age_at_disease_phase"]
    if "course_number" in event and event["course_number"]:
        main_dict["course_number"] = event["course_number"]
    if "disease_phase_number" in event and event["disease_phase_number"]:
        main_dict["disease_phase_number"] = event["disease_phase_number"]
    if "year_at_disease_phase" in event and event["year_at_disease_phase"]:
        main_dict["year_at_disease_phase"] = event["year_at_disease_phase"]
    if "timing_type" in event and event["timing_type"]:
        main_dict["timing_type"] = event["timing_type"]
    if "course" in event and event["course"]:
        main_dict["course"] = event["course"]
    if "disease_phase" in event and event["disease_phase"]:
        main_dict["disease_phase"] = event["disease_phase"]


def generate_subject_json(file_path):
    ignored_entities = []
    subjects = []

    timings = load_timings(file_path)
    # 	del subject_submitter_id
    # del timing_id

    for type in types:
        # Load from local file
        tsvfile = open(file_path + "gen3_" + type + ".tsv")

        reader = csv.DictReader(tsvfile, dialect="excel-tab")

        for row in reader:
            # TODO add clinical events
            # TODO check cause of death mapping
            if type == "person":
                new_subject = {}
                new_subject["person_id"] = row["*submitter_id"]
                new_subject["ethnicity"] = row["ethnicity"]
                new_subject["race"] = row["race"]
                new_subject["sex"] = row["sex"]
                subjects.append(new_subject)
            elif type == "subject":
                subject = get_subject_by_id(
                    row["*persons.submitter_id"], "person_id", subjects
                )
                if subject == None:
                    subject = {}
                    ignored_entities.append(subject)

                subject["subject_submitter_id"] = row["*submitter_id"]
                subject["honest_broker_subject_id"] = row["*honest_broker_subject_id"]
                subject["project_id"] = row["project_id"]
                if "age_at_enrollment" in row and row["age_at_enrollment"]:
                    subject["age_at_enrollment"] = toNum(row["age_at_enrollment"])
                if "year_at_enrollment" in row and row["year_at_enrollment"]:
                    subject["year_at_enrollment"] = toNum(row["year_at_enrollment"])
                subject["consortium"] = row["consortium"]
                subject["data_contributor_id"] = row["*data_contributor_id"]
                if "enrolled_status" in row:
                    subject["enrolled_status"] = row["enrolled_status"]
                subject["treatment_arm"] = row["treatment_arm"]
                subject["censor_status"] = row["censor_status"]
                if "age_at_censor_status" in row and row["age_at_censor_status"]:
                    subject["age_at_censor_status"] = toNum(row["age_at_censor_status"])

                ### TEMPORARY PATCH TO INClCLUDE YEAR_AT_DISEASE_PHASE
                events = [
                    d
                    for d in timings
                    if "subject_submitter_id" in d
                    and d["subject_submitter_id"] == subject["subject_submitter_id"]
                ]
                if len(events) > 0:
                    tmp_year = None
                    for ev in events:
                        if (
                            "year_at_disease_phase" in ev
                            and ev["year_at_disease_phase"]
                        ):
                            if not tmp_year or ev["year_at_disease_phase"] != tmp_year:
                                tmp_year = ev["year_at_disease_phase"]
                    if tmp_year:
                        subject["year_at_disease_phase"] = tmp_year
                ### END PATCH

                [program_name, project_code] = row["project_id"].split("-")
                subject["auth_resource_path"] = (
                    "/programs/"
                    + program_name
                    + "/projects/"
                    + project_code
                    + "/persons/"
                    + row["*persons.submitter_id"]
                    + "/subjects/"
                    + row["*submitter_id"]
                )
            elif type == "study":
                subject = get_subject_by_id(
                    row["*subjects.submitter_id"], "subject_submitter_id", subjects
                )
                if subject == None:
                    subject = {}
                    ignored_entities.append(subject)
                study = {}
                study["study_id"] = row["study_id"]
                if "study_phase" in row:
                    study["study_phase"] = row["study_phase"]

                if "study_type" in row:
                    study["study_type"] = row["study_type"]

                if "studies" not in subject:
                    subject["studies"] = []
                subject["studies"].append(study)
            elif type == "molecular_analysis":
                subject = get_subject_by_id(
                    row["*subjects.submitter_id"], "subject_submitter_id", subjects
                )
                if subject == None:
                    subject = {}
                    ignored_entities.append(subject)
                molecular_analysis = {}
                if (
                    "age_at_molecular_analysis" in row
                    and row["age_at_molecular_analysis"]
                ):
                    molecular_analysis["age_at_molecular_analysis"] = toNum(
                        row["age_at_molecular_analysis"]
                    )
                # molecular_analysis["allelic_ratio"] = toNum(row["allelic_ratio"])
                if "indepen_abb" in row and row["indepen_abb"]:
                    molecular_analysis["indepen_abb"] = toNum(row["indepen_abb"])
                if "num_chromosomes" in row and row["num_chromosomes"]:
                    molecular_analysis["num_chromosomes"] = toNum(
                        row["num_chromosomes"]
                    )
                if "num_metaphases" in row and row["num_metaphases"]:
                    molecular_analysis["num_metaphases"] = toNum(row["num_metaphases"])
                # molecular_analysis["aa_mutation"] = row["aa_mutation"]
                if "anaplasia" in row and row["anaplasia"]:
                    molecular_analysis["anaplasia"] = row["anaplasia"]
                if "molecular_analysis_classification" in row:
                    molecular_analysis["molecular_analysis_classification"] = row[
                        "molecular_analysis_classification"
                    ]
                if "anaplasia_extent" in row and row["anaplasia_extent"]:
                    molecular_analysis["anaplasia_extent"] = row["anaplasia_extent"]
                if "chromosome" in row:
                    molecular_analysis["chromosome"] = row["chromosome"]
                if "dna_index" in row and row["dna_index"]:
                    molecular_analysis["dna_index"] = row["dna_index"]
                if "dna_index_numeric" in row and row["dna_index_numeric"]:
                    molecular_analysis["dna_index_numeric"] = toNum(
                        row["dna_index_numeric"]
                    )
                if "gene1" in row and row["gene1"]:
                    molecular_analysis["gene1"] = row["gene1"]
                if "gene2" in row and row["gene2"]:
                    molecular_analysis["gene2"] = row["gene2"]
                if "iscn" in row:
                    molecular_analysis["iscn"] = row["iscn"]
                if "mutation_type" in row:
                    molecular_analysis["mutation_type"] = row["mutation_type"]
                if "genetic_seq" in row:
                    molecular_analysis["genetic_seq"] = row["genetic_seq"]
                if "cytodifferentiation" in row:
                    molecular_analysis["cytodifferentiation"] = row[
                        "cytodifferentiation"
                    ]
                if "karyotype_status" in row:
                    molecular_analysis["karyotype_status"] = row["karyotype_status"]
                if "mitoses" in row:
                    molecular_analysis["mitoses"] = row["mitoses"]
                if "molecular_abnormality" in row and row["molecular_abnormality"]:
                    molecular_analysis["molecular_abnormality"] = row[
                        "molecular_abnormality"
                    ]
                if (
                    "molecular_abnormality_result" in row
                    and row["molecular_abnormality_result"]
                ):
                    molecular_analysis["molecular_abnormality_result"] = row[
                        "molecular_abnormality_result"
                    ]
                if "translocation_status" in row:
                    molecular_analysis["translocation_status"] = row[
                        "translocation_status"
                    ]
                if "molecular_analysis_method" in row:
                    molecular_analysis["molecular_analysis_method"] = row[
                        "molecular_analysis_method"
                    ]
                if "variant_type" in row:
                    molecular_analysis["variant_type"] = row["variant_type"]
                if "molecular_analysis_sample_source" in row:
                    molecular_analysis["molecular_analysis_sample_source"] = row[
                        "molecular_analysis_sample_source"
                    ]

                event = get_timing_by_ids(
                    row["*subjects.submitter_id"], row["timings.submitter_id"], timings
                )
                if event:
                    flatten_timing(molecular_analysis, event)

                if "molecular_analysis" not in subject:
                    subject["molecular_analysis"] = []
                subject["molecular_analysis"].append(molecular_analysis)
            elif type == "tumor_assessment":
                subject = get_subject_by_id(
                    row["*subjects.submitter_id"], "subject_submitter_id", subjects
                )
                if subject == None:
                    subject = {}
                    ignored_entities.append(subject)
                tumor_assessment = {}
                if row["age_at_tumor_assessment"]:
                    tumor_assessment["age_at_tumor_assessment"] = toNum(
                        row["age_at_tumor_assessment"]
                    )
                if "longest_diam_dim1" in row and row["longest_diam_dim1"]:
                    tumor_assessment["longest_diam_dim1"] = toNum(
                        row["longest_diam_dim1"]
                    )
                else:
                    tumor_assessment["longest_diam_dim1"] = None
                if "longest_diam_dim2" in row and row["longest_diam_dim2"]:
                    tumor_assessment["longest_diam_dim2"] = toNum(
                        row["longest_diam_dim2"]
                    )
                if "longest_diam_dim3" in row and row["longest_diam_dim3"]:
                    tumor_assessment["longest_diam_dim3"] = toNum(
                        row["longest_diam_dim3"]
                    )
                if "invasiveness" in row and row["invasiveness"]:
                    tumor_assessment["invasiveness"] = row["invasiveness"]
                if "nodal_clinical" in row and row["nodal_clinical"]:
                    tumor_assessment["nodal_clinical"] = row["nodal_clinical"]
                if "nodal_involvement" in row:
                    tumor_assessment["nodal_involvement"] = row["nodal_involvement"]
                if "nodal_pathology" in row and row["nodal_pathology"]:
                    tumor_assessment["nodal_pathology"] = row["nodal_pathology"]
                if "nodal_site" in row:
                    tumor_assessment["nodal_site"] = row["nodal_site"]
                if "parameningeal_extension" in row and row["parameningeal_extension"]:
                    tumor_assessment["parameningeal_extension"] = row[
                        "parameningeal_extension"
                    ]
                if "tumor_classification" in row and row["tumor_classification"]:
                    tumor_assessment["tumor_classification"] = row[
                        "tumor_classification"
                    ]
                if "tumor_detection_method" in row:
                    tumor_assessment["tumor_detection_method"] = row[
                        "tumor_detection_method"
                    ]
                if "tumor_laterality" in row:
                    tumor_assessment["tumor_laterality"] = row["tumor_laterality"]
                if "tumor_site" in row and row["tumor_site"]:
                    tumor_assessment["tumor_site"] = row["tumor_site"]
                tumor_assessment["tumor_site_other"] = row["tumor_site_other"]
                if "tumor_size" in row and row["tumor_size"]:
                    tumor_assessment["tumor_size"] = row["tumor_size"]
                if "tumor_state" in row:
                    tumor_assessment["tumor_state"] = row["tumor_state"]
                if "site_within_bone" in row:
                    tumor_assessment["site_within_bone"] = row["site_within_bone"]
                if "facture" in row:
                    tumor_assessment["facture"] = row["facture"]
                if "computed_volume" in row:
                    tumor_assessment["computed_volume"] = row["computed_volume"]

                event = get_timing_by_ids(
                    row["*subjects.submitter_id"], row["timings.submitter_id"], timings
                )
                if event:
                    flatten_timing(tumor_assessment, event)

                if "tumor_assessments" not in subject:
                    subject["tumor_assessments"] = []
                subject["tumor_assessments"].append(tumor_assessment)
            elif type == "survival_characteristic":
                subject = get_subject_by_id(
                    row["*subjects.submitter_id"], "subject_submitter_id", subjects
                )
                if subject == None:
                    subject = {}
                    ignored_entities.append(subject)
                survival_characteristic = {}
                if row["age_at_lkss"]:
                    survival_characteristic["age_at_lkss"] = toNum(row["age_at_lkss"])
                else:
                    survival_characteristic["age_at_lkss"] = None
                survival_characteristic["lkss"] = row["lkss"]
                if row["cause_of_death"]:
                    survival_characteristic["cause_of_death"] = toArray(
                        row["cause_of_death"]
                    )

                event = get_timing_by_ids(
                    row["*subjects.submitter_id"], row["timings.submitter_id"], timings
                )
                if event:
                    flatten_timing(survival_characteristic, event)

                if "survival_characteristics" not in subject:
                    subject["survival_characteristics"] = []
                subject["survival_characteristics"].append(survival_characteristic)
            elif type == "staging":
                subject = get_subject_by_id(
                    row["*subjects.submitter_id"], "subject_submitter_id", subjects
                )
                if subject == None:
                    subject = {}
                    ignored_entities.append(subject)
                staging = {}
                if row["age_at_staging"]:
                    staging["age_at_staging"] = toNum(row["age_at_staging"])
                if "irs_group" in row and row["irs_group"]:
                    staging["irs_group"] = row["irs_group"]

                if "stage" in row and row["stage"]:
                    staging["stage"] = row["stage"]
                if "stage_system" in row and row["stage_system"]:
                    staging["stage_system"] = row["stage_system"]

                if "tnm_finding" in row and row["tnm_finding"]:
                    staging["tnm_finding"] = row["tnm_finding"]

                event = get_timing_by_ids(
                    row["*subjects.submitter_id"], row["timings.submitter_id"], timings
                )
                if event:
                    flatten_timing(staging, event)

                if "stagings" not in subject:
                    subject["stagings"] = []
                subject["stagings"].append(staging)
            elif type == "histology":
                subject = get_subject_by_id(
                    row["*subjects.submitter_id"], "subject_submitter_id", subjects
                )
                if subject == None:
                    subject = {}
                    ignored_entities.append(subject)
                histology = {}
                if row["age_at_hist_assessment"]:
                    histology["age_at_hist_assessment"] = toNum(
                        row["age_at_hist_assessment"]
                    )
                if (
                    "histology_result_numeric" in row
                    and row["histology_result_numeric"]
                ):
                    histology["histology_result_numeric"] = toNum(
                        row["histology_result_numeric"]
                    )
                if "hist_ICD_O_morph_code" in row:
                    histology["hist_ICD_O_morph_code"] = row["hist_ICD_O_morph_code"]
                if "hist_assessment_review" in row:
                    histology["hist_assessment_review"] = row["hist_assessment_review"]
                if "histology" in row and row["histology"]:
                    histology["histology"] = row["histology"]
                if "histology_grade" in row and row["histology_grade"]:
                    histology["histology_grade"] = row["histology_grade"]
                if "histology_inpc" in row and row["histology_inpc"]:
                    histology["histology_inpc"] = row["histology_inpc"]
                if "histology_result" in row:
                    histology["histology_result"] = row["histology_result"]
                if "histology_result_unit" in row:
                    histology["histology_result_unit"] = row["histology_result_unit"]
                if "mature_glial_implants" in row:
                    histology["mature_glial_implants"] = row["mature_glial_implants"]
                if "somatic_malignancy_type" in row:
                    histology["somatic_malignancy_type"] = row[
                        "somatic_malignancy_type"
                    ]

                event = get_timing_by_ids(
                    row["*subjects.submitter_id"], row["timings.submitter_id"], timings
                )
                if event:
                    flatten_timing(histology, event)

                if "histologies" not in subject:
                    subject["histologies"] = []
                subject["histologies"].append(histology)
            elif type == "secondary_malignant_neoplasm":
                subject = get_subject_by_id(
                    row["*subjects.submitter_id"], "subject_submitter_id", subjects
                )
                if subject == None:
                    subject = {}
                    ignored_entities.append(subject)
                secondary_malignant_neoplasm = {}
                if row["age_at_smn"]:
                    secondary_malignant_neoplasm["age_at_smn"] = toNum(
                        row["age_at_smn"]
                    )

                secondary_malignant_neoplasm["smn_morph_icdo"] = row["smn_morph_icdo"]
                if "smn_morph_sno" in row:
                    secondary_malignant_neoplasm["smn_morph_sno"] = row["smn_morph_sno"]
                if "smn_morph_txt" in row:
                    secondary_malignant_neoplasm["smn_morph_txt"] = row["smn_morph_txt"]
                secondary_malignant_neoplasm["smn_top_icdo"] = row["smn_top_icdo"]
                if "smn_top_sno" in row:
                    secondary_malignant_neoplasm["smn_top_sno"] = row["smn_top_sno"]
                if "smn_top_txt" in row:
                    secondary_malignant_neoplasm["smn_top_txt"] = row["smn_top_txt"]
                if "smn_yn" in row:
                    secondary_malignant_neoplasm["smn_yn"] = row["smn_yn"]
                # secondary_malignant_neoplasm["smn_site"] = row["smn_site"]

                event = get_timing_by_ids(
                    row["*subjects.submitter_id"], row["timings.submitter_id"], timings
                )
                if event:
                    flatten_timing(secondary_malignant_neoplasm, event)

                if "secondary_malignant_neoplasm" not in subject:
                    subject["secondary_malignant_neoplasm"] = []
                subject["secondary_malignant_neoplasm"].append(
                    secondary_malignant_neoplasm
                )
            elif type == "biopsy_surgical_procedure":
                subject = get_subject_by_id(
                    row["*subjects.submitter_id"], "subject_submitter_id", subjects
                )
                if subject == None:
                    subject = {}
                    ignored_entities.append(subject)
                biopsy_surgical_procedure = {}
                if row["age_at_procedure"]:
                    biopsy_surgical_procedure["age_at_procedure"] = toNum(
                        row["age_at_procedure"]
                    )

                biopsy_surgical_procedure["margins"] = row["margins"]
                biopsy_surgical_procedure["procedure_site"] = row["procedure_site"]
                biopsy_surgical_procedure["procedure_tumor_classification"] = row[
                    "procedure_tumor_classification"
                ]
                biopsy_surgical_procedure["procedure_type"] = row["procedure_type"]

                event = get_timing_by_ids(
                    row["*subjects.submitter_id"], row["timings.submitter_id"], timings
                )
                if event:
                    flatten_timing(biopsy_surgical_procedure, event)

                if "biopsy_surgical_procedures" not in subject:
                    subject["biopsy_surgical_procedures"] = []
                subject["biopsy_surgical_procedures"].append(biopsy_surgical_procedure)
            elif type == "lab":
                subject = get_subject_by_id(
                    row["*subjects.submitter_id"], "subject_submitter_id", subjects
                )
                if subject == None:
                    subject = {}
                    ignored_entities.append(subject)
                lab = {}
                # if row["age_at_lab"]:
                # 	lab["age_at_lab"] = toNum(row["age_at_lab"])
                if row["lab_result_numeric"]:
                    lab["lab_result_numeric"] = toNum(row["lab_result_numeric"])
                # if row["pmid_ref"]:
                # 	lab["pmid_ref"] = toNum(row["pmid_ref"])
                # if row["threshold_high"]:
                # 	lab["threshold_high"] = toNum(row["threshold_high"])
                # if row["threshold_low"]:
                # 	lab["threshold_low"] = toNum(row["threshold_low"])

                # lab["lab_cat"] = row["lab_cat"]
                # lab["lab_method"] = row["lab_method"]
                # lab["lab_result"] = row["lab_result"]
                # lab["lab_result_unit"] = row["lab_result_unit"]
                # lab["lab_seq_method"] = row["lab_seq_method"]
                # lab["lab_spec_type"] = row["lab_spec_type"]
                lab["lab_test"] = row["lab_test"]

                event = get_timing_by_ids(
                    row["*subjects.submitter_id"], row["timings.submitter_id"], timings
                )
                if event:
                    flatten_timing(lab, event)

                if "labs" not in subject:
                    subject["labs"] = []
                subject["labs"].append(lab)
            elif type == "disease_characteristic":
                subject = get_subject_by_id(
                    row["*subjects.submitter_id"], "subject_submitter_id", subjects
                )
                if subject == None:
                    subject = {}
                    ignored_entities.append(subject)
                disease_characteristic = {}
                # if row["age_at_cytology"]:
                # 	disease_characteristic["age_at_cytology"] = toNum(row["age_at_cytology"])

                disease_characteristic["mki"] = row["mki"]

                if (
                    "initial_treatment_category" in row
                    and row["initial_treatment_category"]
                ):
                    disease_characteristic["initial_treatment_category"] = row[
                        "initial_treatment_category"
                    ]

                # disease_characteristic["all_type"] = row["all_type"]
                # disease_characteristic["detection_method"] = row["detection_method"]
                # disease_characteristic["fab_type"] = row["fab_type"]
                # disease_characteristic["igccc_risk_group"] = row["igccc_risk_group"]
                # disease_characteristic["magic_risk_group"] = row["magic_risk_group"]
                # disease_characteristic["who_aml"] = row["who_aml"]

                event = get_timing_by_ids(
                    row["*subjects.submitter_id"], row["timings.submitter_id"], timings
                )
                if event:
                    flatten_timing(disease_characteristic, event)

                if "disease_characteristics" not in subject:
                    subject["disease_characteristics"] = []
                subject["disease_characteristics"].append(disease_characteristic)
            elif type == "external_reference":
                subject = get_subject_by_id(
                    row["*subjects.submitter_id"], "subject_submitter_id", subjects
                )
                if subject == None:
                    subject = {}
                    ignored_entities.append(subject)
                external_reference = {}
                # if row["age_at_cytology"]:
                # 	external_reference["age_at_cytology"] = toNum(row["age_at_cytology"])

                external_reference["external_resource_icon_path"] = row[
                    "external_resource_icon_path"
                ]
                external_reference["external_resource_id"] = row["external_resource_id"]
                external_reference["external_resource_name"] = row[
                    "external_resource_name"
                ]
                external_reference["external_subject_id"] = row["external_subject_id"]
                external_reference["external_subject_submitter_id"] = row[
                    "external_subject_submitter_id"
                ]
                external_reference["external_subject_url"] = row["external_subject_url"]
                external_reference["external_links"] = row["external_links"]

                if "external_references" not in subject:
                    subject["external_references"] = []
                subject["external_references"].append(external_reference)

    for subject in subjects:
        del subject["person_id"]
        # for key, values in subject.items():
        # 	if isinstance(values, list):
        # 		for value in values:
        # 			if "timings" in value:
        # 				for event in value["timings"]:
        # 					if "subject_submitter_id" in event:
        # 						del event["subject_submitter_id"]
        # 					if "timing_id" in event:
        # 						del event["timing_id"]

    # print(subjects)
    # print(len(subjects))
    # print(len(ignored_entities))
    return subjects

    # try:
    # 	# TODO verify values has length == 2...
    # 	values = entity["project_id"].split("-")
    # 	program_name = values[0]
    # 	project_code = values[1]
    # 	del entity["project_id"]
    # 	response = sub.submit_record(program_name, project_code, entity)
    # except requests.HTTPError as exception:
    # 	print(exception)
    # 	print(exception.response.status_code)
    # 	print(exception.response.content)
    # 	print(entity)
    # 	failed.append(entity["submitter_id"])
    # 	if exception.response.status_code == 400:
    # 		print("fix tsv file data")
