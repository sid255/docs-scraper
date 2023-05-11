import elasticsearch
from elasticsearch.exceptions import NotFoundError
from elasticsearch import helpers
import sys

from datetime import datetime

def remove_bad_encoding(value):
    return value.replace('&#x27;', "'")

def clean_one_field(value):
    if isinstance(value, bool):
        return str(value)
    elif isinstance(value, str):
        return remove_bad_encoding(value)
    return value

def clean_dict(record):
    for key, value in record.items():
        if isinstance(value, dict):
            record[key] = clean_dict(value)
        else:
            record[key] = clean_one_field(value)
    return record

def parse_record(record):
    new_weight = {}
    for k, v in record['weight'].items():
        new_weight[k] = v
    new_hierarchy = {}
    for k, v in record['hierarchy'].items():
        new_hierarchy['hierarchy_' + k] = v
    new_hierarchy_radio = {}
    for k, v in record['hierarchy_radio'].items():
        key = 'hierarchy_radio_' + k
        new_hierarchy_radio = {**{key: v}, **new_hierarchy_radio}
    del record['weight']
    del record['hierarchy']
    del record['hierarchy_radio']
    del record['hierarchy_camel']
    del record['hierarchy_radio_camel']
    del record['content_camel']
    return {**record, **new_weight, **new_hierarchy, **new_hierarchy_radio}

class ElasticSearchHelper:

    def __init__(self, host, username, password, veify_certs=False, ca_cert_path=None):
        self.client = elasticsearch.Elasticsearch(
            hosts=host,
            ca_certs=ca_cert_path,
            basic_auth=(username, password),
            verify_certs=veify_certs
        )
       
    
    def create_index(self, index):
        try:
            self.client.indices.get(index=index)
        except NotFoundError:
            self.client.indices.create(index=index)
        except Exception as e:
            sys.exit(e)
    
    def ingest_data_into_index(self,index,data_dict):
        try:
            response=self.client.index(
                index=index,
                document=data_dict
            )
        except Exception as e:
            print("ingestion failed to elastic search: ", e)
            return

        return response
    
    def bulk_ingest_data_into_index(self,index,records):
        try:
            actions = [
                {
                    "_index": index,
                    # "_type": "tickets",
                    # "_id": j,
                    "_source": {
                        "data": record,
                        "timestamp": datetime.now()}
                }
                    for record in records
                ]
            
            response=helpers.bulk(self.client, actions=actions)

        except Exception as e:
            print("ingestion failed to elastic search: ", e)
            return

        return response
    
    def get_documents_from_index(self, index):
        response = self.client.search(index=index)

    def add_records(self, records, url, from_sitemap):
        """Add new records to the index"""

        record_count = len(records)
        for i in range(0, record_count, 50):
            parsed_records = list(map(parse_record, records[i:i + 50]))
            cleaned_records = list(map(clean_dict, parsed_records))
            
            self.bulk_ingest_data_into_index("es_index_00",cleaned_records)
            

        color = "96" if from_sitemap else "94"

        print(
            f'\033[{color}m> Docs-Scraper: \033[0m{url}\033[93m {record_count} records\033[0m)')

        


# index = "es_index_00",
# esh = ElasticSearchHelper(
#     "https://localhost:9200",
#     "elastic",
#     "UG9d5rs8Grfrkwy7jpsy",
#     False,
#     None
#     )

# esh.create_index(index)

# esh.ingest_data_into_index(index, {"test":"data"})