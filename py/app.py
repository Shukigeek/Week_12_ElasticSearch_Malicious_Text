import os
import time
from sklearn.datasets import fetch_20newsgroups
from elasticsearch import Elasticsearch


class ElasticDemo:
    def __init__(self, host=None, index_name="demo"):
        self.es = Elasticsearch(host or os.getenv("ES_HOST", "http://127.0.0.1:9200"))
        self.index = index_name

    def wait_for_es(self):
        """ממתין עד ש-Elasticsearch מוכן"""
        while True:
            if self.es.ping():
                print("✅ Elasticsearch is up!")
                break
            print("⏳ Waiting for Elasticsearch...")
            time.sleep(2)

    def create_index(self):
        """יוצר אינדקס אם לא קיים"""
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
            print(f"📦 Created index '{self.index}'")

    def index_documents(self, docs):
        """מאנדקס רשימת מסמכים"""
        for i, doc in enumerate(docs, 1):
            self.es.index(index=self.index,id=str(i), document=doc)
            if i % 100 == 0:
                print(f"📥 Indexed document {i}")
        self.es.indices.refresh(index=self.index)
        print("🔄 Index refreshed")

    def search(self, query="space", size=10):
        """חיפוש טקסטואלי (ברירת מחדל 10 תוצאות ראשונות)"""
        res = self.es.search(
            index=self.index,
            query={"match": {"body": query}},
            size=size
        )
        print(f"🔍 Found {res['hits']['total']['value']} results for '{query}':")
        for hit in res["hits"]["hits"]:
            print(f" - {hit["_id"]} {hit['_source']['title']} [{hit['_source']['tag']}]")
    def delete_all_index(self):
        self.es.indices.delete(index="demo")


if __name__ == '__main__':
    # טוענים דאטה
    cats = ['alt.atheism', 'sci.space']
    newsgroups = fetch_20newsgroups(categories=cats)

    docs = []
    for text, label in zip(newsgroups.data, newsgroups.target):
        docs.append({
            "title": text.split("\n")[0][:50],  # שורה ראשונה ככותרת
            "body": text,
            "tag": newsgroups.target_names[label]
        })

    # מריצים את הדמו
    demo = ElasticDemo()
    # demo.delete_all_index()
    demo.wait_for_es()
    demo.create_index()
    demo.index_documents(docs)
    demo.search(query="space", size=5)  # מציג רק 5 תוצאות
