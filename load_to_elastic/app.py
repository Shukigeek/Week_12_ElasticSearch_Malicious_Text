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
        raise Exception(f"data didn't change good to dict error: {e}")



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
                        "TweetID": {"type": "keyword"},
                        "CreateDate": {"type": "date", "format": "yyyy-MM-dd HH:mm:ssZ"},
                        "Antisemitic": {"type": "boolean"},
                        "text": {"type": "text"}
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
    # no need for this project
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
    # no need for this project
    def delete_all_index(self):
        # delete all index
        if self.es.indices.exists(index=self.index):
            self.es.indices.delete(index=self.index)
            print("all index is delete")
    # no need for this project
    def add_document(self, doc, doc_id=None):
        # adding new doc
        response = self.es.index(
            index=self.index,
            id=doc_id,
            document=doc
        )
        print(f"Document added with _id: {response['_id']}")
        return response

    def get_all_documents(self, size=10000):
        # returning all documents
        res = self.es.search(
            index=self.index,
            query={"match_all": {}},
            size=size
        )
        all_docs = res["hits"]["hits"]
        print(f"Total documents retrieved: {len(all_docs)}")
        return all_docs
    def add_field_to_document(self, doc_id, new_field_name, new_field_value):
        # adding new field to a doc
        if self.es.exists(index=self.index, id=doc_id):
            self.es.update(
                index=self.index,
                id=doc_id,
                body={
                    "doc": {
                        new_field_name: new_field_value
                    }
                }
            )
            print(f"Added field '{new_field_name}' to document {doc_id}")
        else:
            print(f"Document {doc_id} does not exist")
    # no need for this project
    def delete_doc_by_id(self, doc_id):
        # delete doc by is id
        if self.es.exists(index=self.index, id=doc_id):
            self.es.delete(index=self.index, id=doc_id)
            print(f"Document {doc_id} deleted from index '{self.index}'")
        else:
            print(f"Document {doc_id} does not exist in index '{self.index}'")

    def delete_docs_by_query(self, query):
        """
        Delete documents from the index that match the given query.
        :param query: dict representing the Elasticsearch query
        """
        response = self.es.delete_by_query(
            index=self.index,
            body={"query": query}
        )
        deleted = response.get('deleted', 0)
        print(f"Deleted {deleted} documents from index '{self.index}'")


if __name__ == '__main__':
    tweets = csv_to_dict()


    elastic = Elastic()
    # demo.delete_all_index()
    # elastic.wait_for_es()
    # elastic.create_index()
    # elastic.index_documents(tweets)
    # elastic.search(query="israel", size=5)
    print(elastic.get_all_documents()[:1])