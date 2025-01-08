import json
from elasticsearch import Elasticsearch

from build_json import file_path, generate_subject_json


# config.ES = {
#     "es.nodes": "localhost",
#     "es.port": "9200",
#     "es.input.json": "yes",
#     "es.nodes.client.only": "false",
#     "es.nodes.discovery": "false",
#     "es.nodes.data.only": "false",
#     "es.nodes.wan.only": "true",
# }


def get_es():
        """
        Create ElasticSearch instance
        :return:
        """
        es_hosts = "localhost"
        es_port = "9200"
        return Elasticsearch([{"host": es_hosts, "port": es_port}])

# def generate_mapping(self, doc_name, field_types):
#         """
#         :param doc_name: name of the Elasticsearch document to create mapping for
#         :param field_types: dictionary of field and their types
#         :return: JSON with proper mapping to be used in Elasticsearch
#         """
#         es_type = {str: "keyword", float: "float", int: "long"}

#         properties = {
#             k: {"type": es_type[v[0]]}
#             if v[0] is not str
#             else {"type": es_type[v[0]], "fields": {"analyzed": {"type": "text"}}}
#             for k, v in list(field_types.items())
#         }

#         # explicitly mapping 'node_id'
#         properties["node_id"] = {"type": "keyword"}

#         mapping = {"mappings": {doc_name: {"properties": properties}}}
#         return mapping

# def write_df(self, df, index, doc_type, types):
#         """
#         Function to write the data frame to ElasticSearch
#         :param df: data frame to be written
#         :param index: name of the index
#         :param doc_type: document type's name
#         :param types:
#         :return:
#         """
#         print("LUCA TUBE")
#         for x in df.collect():
#             print (x)
#         print("space --")
#         print(index)
#         print(types)
#         print(doc_type)
#         try:
#             for plugin in post_process_plugins:
#                 df = df.map(lambda x: plugin(x))

#             types = add_auth_resource_path_mapping(types)
#             mapping = self.generate_mapping(doc_type, types)

#             self.reset_status()
#             index_to_write = self.versioning.create_new_index(
#                 mapping, self.versioning.get_next_index_version(index)
#             )
#             self.write_to_new_index(df, index_to_write, doc_type)
#             self.versioning.putting_new_version_tag(index_to_write, index)
#             putting_timestamp(self.es, index_to_write)
#             self.reset_status()
#         except Exception as e:
#             print(e)

# def create_guppy_array_config(self, etl_index_name, types):
        """
        Create index with Guppy configuration for array fields
        :param etl_index_name:
        :param types:
        """
        # index = "{}-array-config".format(etl_index_name)
        # alias = "{}_array-config".format(etl_index_name.split("_")[0])

        # mapping = {
        #     "mappings": {
        #         "_doc": {
        #             "properties": {
        #                 "timestamp": {"type": "date"},
        #                 "array": {"type": "keyword"},
        #             }
        #         }
        #     }
        # }

        # latest_transaction_time = get_latest_utc_transaction_time()

        # doc = {
        #     "timestamp": latest_transaction_time,
        #     "array": ["{}".format(k) for k, v in list(types.items()) if v[1]],
        # }

        # try:
        #     self.reset_status()
        #     index_to_write = self.versioning.create_new_index(
        #         mapping, self.versioning.get_next_index_version(index)
        #     )
        #     self.es.index(index_to_write, "_doc", id=etl_index_name, body=doc)
        #     self.versioning.putting_new_version_tag(index_to_write, index)
        #     self.versioning.putting_new_version_tag(index_to_write, alias)
        #     putting_timestamp(self.es, index_to_write)
        #     self.reset_status()
        # except Exception as e:
        #     print(e)



# Create pcdc_1
es = get_es()

index = "pcdc_20220110_1"
# index = "pcdc_20220501_2"
alias = "pcdc"


# doc = {
#           "subject_submitter_id" : "0x908BB2F15F92F3C17704F4C61A2BE55A",
#           "age_at_lkss" : 8769,
#           "auth_resource_path" : "/programs/pcdc/projects/20210325/persons/person_156939/subjects/0x908BB2F15F92F3C17704F4C61A2BE55A",
#           "_study_count" : 1,
#           "_molecular_analysis_count" : 2,
#           "consortium" : "INSTRuCT",
#           "tests": [
#                 {
#                     "age_at_test": 3456,
#                     "test_result": "positive"
#                 },
#                 {
#                     "age_at_test": 5698,
#                     "test_result": "negative"
#                 }
#             ]
#         }


with open('../files/nested_mapping.json') as mapping_f:
  # returns JSON object as 
  # a dictionary
  mapping =  json.load(mapping_f)



  docs = None

  docs = generate_subject_json(file_path)
  # with open('../files/es_data_prod.json',) as f:
  #   docs = json.load(f)

  if docs is None:
    print("error loading file/generating json object.")
    exit()

  request_body = {
    "settings" : {"number_of_shards": 1, "number_of_replicas": 1, "index.mapping.total_fields.limit": 2000}
  }
  request_body.update(mapping)
  es.indices.create(index=index, body=request_body)
  es.indices.put_alias(index=index, name=alias)

  # print(docs)
  i = 1
  for doc in docs:
    idx = "subj_" + str(i)
    es.index(index, "subject", id=idx, body=doc)
    i = i + 1




#Create Array-config
index = "pcdc_20220110_1-array-config"
# index = "pcdc_20220501_2-array-config"
alias = "pcdc-array-config"

mapping = {
          "mappings" : {
            "_doc" : {
                "properties" : {
                  "array" : {
                    "type" : "keyword"
                  },
                  "timestamp" : {
                    "type" : "date"
                  }
                }
              }
          }
        }

doc = {
        "timestamp" : "2021-04-29T16:56:06.490549",
        "array" : [
            "studies",
            "molecular_analysis",
            "tumor_assessments",
            "survival_characteristics",
            "stagings",
            "histologies",
            "secondary_malignant_neoplasm",
            "biopsy_surgical_procedures",
            "labs",
            "disease_characteristics",
            "external_references",
            "radiation_therapies",
            "subject_responses",
            "total_doses"
        ]
      }

request_body = {"settings" : {"number_of_shards": 1, "number_of_replicas": 1}}
request_body.update(mapping)
es.indices.create(index=index, body=request_body)
es.index(index, "_doc", id="pcdc", body=doc)
es.indices.put_alias(index=index, name=alias)





