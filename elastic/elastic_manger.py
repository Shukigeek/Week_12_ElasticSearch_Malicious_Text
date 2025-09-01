from elastic.elastic_base import ElasticBase

class ElasticManager:
    def __init__(self, host=None, index_name="malicious_tweets"):
        self.elastic = ElasticBase(host, index_name)
        self.elastic.wait_for_es()

    def setup_index(self, docs):
        self.elastic.create_index()
        self.elastic.index_documents(docs)

    def add_field_bulk(self, docs, field_name, func):
        actions = []
        for doc in docs:
            doc_id = doc["_id"]
            text = doc["_source"]["text"]
            new_value = func(text)
            if new_value is None:
                new_value = "" if field_name != "weapons" else []
            actions.append({
                "_op_type": "update",
                "_index": self.elastic.index,
                "_id": doc_id,
                "doc": {field_name: new_value}
            })
        self.elastic.bulk_update(actions)

    def get_all(self, size=10000):
        docs = self.elastic.get_all_documents(size)
        print(f"Retrieved {len(docs)} documents")
        return docs

    def delete_docs(self, query):
        deleted = self.elastic.delete_docs_by_query(query)
        print(f"Deleted {deleted} documents from index '{self.elastic.index}'")
