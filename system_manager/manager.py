import os
from csv_to_dict.load_csv import CSVLoader
from process.sentiment import Sentiment
from process.detect_weapon import WeaponDetector
from elastic.elastic_manger import ElasticManager


class Manager:
    def __init__(self):
        file_path = os.getenv("CSV_PATH", "data/tweets_injected.csv")
        self.documents = CSVLoader(file_path).load()
        self.es = ElasticManager(
            host=os.getenv("ES_HOST", "http://127.0.0.1:9200"),
            index_name="malicious_tweets"
        )
        self.all_docs = None
        self.load_to_elastic()
        self.add_row_emotions()
        self.add_row_weapon()
        self.delete_docs()

    def load_to_elastic(self):
        self.es.setup_index(self.documents)
        self.all_docs = self.es.get_all()

    def add_row_emotions(self):
        self.es.add_field_bulk(
            docs=self.all_docs,
            field_name="sentiment",
            func=lambda text: Sentiment(text).sentiment()
        )

    def add_row_weapon(self):
        self.es.add_field_bulk(
            docs=self.all_docs,
            field_name="weapons",
            func=lambda text: WeaponDetector(text).detect_weapons()
        )

    def delete_docs(self):
        query = {
            "bool": {
                "must": [
                    {"match": {"Antisemitic": "1"}},
                    {"terms": {"sentiment": ["positive", "neutral"]}}
                ],
                "must_not": [
                    {"exists": {"field": "weapons"}}
                ]
            }
        }
        self.es.delete_docs(query)

    def docs_antisemitic_with_some_weapon(self):

        query = {
            "query": {
                "bool": {
                    "must": [
                        {"match": {"Antisemitic": "1"}},
                        {"exists": {"field": "weapons"}}
                    ]
                }
            }
        }
        return self.es.elastic.es.search(index=self.es.elastic.index, body=query,size=10000)["hits"]["hits"]

    def docs_with_two_weapons(self):
        query = {
            "query": {
                "bool": {
                    "must": [
                        {
                            "script": {
                                "script": {
                                    "source": "doc['weapons'].size() >= 2",
                                    "lang": "painless"
                                }
                            }
                        }
                    ]
                }
            }
        }
        return self.es.elastic.es.search(index=self.es.elastic.index, body=query,size=10000)["hits"]["hits"]


if __name__ == '__main__':

    # es = Elasticsearch("http://127.0.0.1:9200")
    # if es.indices.exists(index="malicious_tweets"):
    #     es.indices.delete(index="malicious_tweets")
    #     print(f"Index malicious_tweets deleted.")
    #
    m = Manager()
    print("אנטישמיים עם כלי נשק:", m.docs_antisemitic_with_some_weapon()[:1])
    print("מסמכים עם שני כלי נשק לפחות:", m.docs_with_two_weapons()[:1])
