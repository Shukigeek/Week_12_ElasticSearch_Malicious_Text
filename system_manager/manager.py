import os

from load_to_elastic.app import Elastic
from csv_to_dict.load_csv import CSVLoader
from process.sentiment import Sentiment
from process.detect_weapon import WeaponDetector


class Manager:
    def __init__(self,documents):
        file_path = os.environ.get("CSV_PATH", "../data/tweets_injected.csv")
        self.documents = CSVLoader(file_path).load()
        self.es = Elastic(host=os.getenv("ES_HOST","http://127.0.0.1:9200")
                          ,index_name="malicious_tweets")
        self.es.wait_for_es()
        self.all_docs = None
    def load_to_elastic(self):
        self.es.create_index()
        self.es.index_documents(self.documents)
        self.all_docs = self.es.get_all_documents()
    def add_row_emotions(self):
        for doc in self.all_docs:
            s = Sentiment(doc["_source"]["text"])
            self.es.add_field_to_document(doc["_id"],"sentiment",s.sentiment())
    def add_row_weapon(self):
        for doc in self.all_docs:
            wd = WeaponDetector(doc["_source"]["text"])
            self.es.add_field_to_document(doc["_id"],"weapons",wd.detect_weapons())
    def delete_docs(self):
        query = {
            "bool": {
                "must": [
                    {"match": {"Antisemitic": True}},
                    {"terms": {"sentiment": ["positive", "neutral"]}}
                ],
                "must_not": [
                    {"exists": {"field": "weapons"}}
                ]
            }
        }
        self.es.delete_docs_by_query(query)
