import os
import time
from elasticsearch import Elasticsearch
import csv

file_path = os.environ.get("CSV_PATH", "../data/tweets_injected.csv")

print("Using:", file_path)

def csv_to_dict(data=file_path):
    try:
        with open(data, newline='', encoding="utf-8") as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception as e:
        raise Exception("data didn't change good to dict")



class Elastic:
    def __init__(self, host=None, index_name="malicious_tweets"):
        self.es = Elasticsearch(host or os.getenv("ES_HOST", "http://127.0.0.1:9200"))
        self.index = index_name

    def wait_for_es(self):
        # waiting to elasticsearch to go up
        while True:
            if self.es.ping():
                print("Elasticsearch is up!")
                break
            print("Waiting for Elasticsearch...")
            time.sleep(2)

    def create_index(self):
        # creating index + mapping only if it not already exists
        if not self.es.indices.exists(index=self.index):
            mapping = {
                "mappings": {
                    "properties": {
                        "title": {"type": "text"},
                        "body": {"type": "text"},
                        "tag": {"type": "keyword"}
                    }
                }
            }
            self.es.indices.create(index=self.index, body=mapping)
            print(f"Created index '{self.index}'")

    def index_documents(self, docs):
        # making index to each document
        for i, doc in enumerate(docs, 1):
            self.es.index(index=self.index,id=str(i), document=doc)
            if i % 100 == 0:
                print(f"Indexed document {i}")
        # making sure that we are going to use up-to-date data
        self.es.indices.refresh(index=self.index)
        print("Index refreshed")

    def search(self, query="israel", size=10):
        # textual search the default is first 10
        # and to search to the word israel
        res = self.es.search(
            index=self.index,
            query={"match": {"body": query}},
            size=size
        )
        print(f"Found {res['hits']['total']['value']} results for '{query}':")
        for hit in res["hits"]["hits"]:
            print(f" - {hit["_id"]} {hit['_source']['title']} [{hit['_source']['tag']}]")
    def delete_all_index(self):
        # delete all index
        if self.es.indices.exists(index=self.index):
            self.es.indices.delete(index=self.index)
            print("all index is delete")

    def delete_doc_by_id(self, doc_id):
        # delete doc by is id
        if self.es.exists(index=self.index, id=doc_id):
            self.es.delete(index=self.index, id=doc_id)
            print(f"Document {doc_id} deleted from index '{self.index}'")
        else:
            print(f"Document {doc_id} does not exist in index '{self.index}'")


if __name__ == '__main__':
    tweets = csv_to_dict()


    elastic = Elastic()
    # demo.delete_all_index()
    elastic.wait_for_es()
    elastic.create_index()
    elastic.index_documents(tweets)
    elastic.search(query="israel", size=5)
