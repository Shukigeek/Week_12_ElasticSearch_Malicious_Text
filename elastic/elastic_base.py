import os
import time
from elasticsearch import Elasticsearch, helpers


class ElasticBase:
    def __init__(self, host=None, index_name="malicious_tweets"):
        self.es = Elasticsearch(host or os.getenv("ES_HOST", "http://127.0.0.1:9200"))
        self.index = index_name

    def wait_for_es(self):
        while True:
            if self.es.ping():
                print("Elasticsearch is up!")
                break
            print("Waiting for Elasticsearch...")
            time.sleep(2)

    def create_index(self):
        if not self.es.indices.exists(index=self.index):
            mapping = {
                "mappings": {
                    "properties": {
                        "TweetID": {"type": "keyword"},
                        "CreateDate": {"type": "keyword"},
                        "Antisemitic": {"type": "keyword"},
                        "text": {"type": "text"},
                        "sentiment": {"type": "keyword"},
                        "weapons": {"type": "keyword"}
                    }
                }
            }
            self.es.indices.create(index=self.index, body=mapping)
            print(f"Created index '{self.index}'")

    def index_documents(self, docs):
        actions = [
            {
                "_op_type": "index",
                "_index": self.index,
                "_id": str(i),
                "_source": doc
            }
            for i, doc in enumerate(docs, 1)
        ]
        helpers.bulk(self.es, actions)
        self.es.indices.refresh(index=self.index)
        print(f"Indexed {len(docs)} documents (bulk)")

    def bulk_update(self, actions):
        helpers.bulk(self.es, actions)
        print(f"Bulk update applied to {len(actions)} documents")

    def get_all_documents(self, size=10000):
        res = self.es.search(
            index=self.index,
            query={"match_all": {}},
            size=size
        )
        return res["hits"]["hits"]

    def update_document(self, doc_id, new_field_name, new_field_value):
        if self.es.exists(index=self.index, id=doc_id):
            self.es.update(
                index=self.index,
                id=doc_id,
                body={"doc": {new_field_name: new_field_value}}
            )
            print(f"Added field '{new_field_name}' to document {doc_id}")
        else:
            print(f"Document {doc_id} does not exist")

    def delete_docs_by_query(self, query):
        response = self.es.delete_by_query(
            index=self.index,
            body={"query": query}
        )
        return response.get('deleted', 0)
