import pymongo

class DocumentDB:
    def __init__(self, uri="mongodb://localhost:27017/", db_name="hetio"):
        self.client = pymongo.MongoClient(uri)
        self.db = self.client[db_name]

    def insert_node(self, node_doc):
        # Upsert the document so that duplicate nodes are not inserted.
        self.db.nodes.update_one({"_id": node_doc["_id"]}, {"$set": node_doc}, upsert=True)

    def close(self):
        self.client.close()
