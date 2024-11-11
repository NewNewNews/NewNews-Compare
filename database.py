from pymongo import MongoClient

class ComparisonDatabase:
    def __init__(self, db_url, db_name, collection_name):
        self.client = MongoClient(db_url)
        self.db = self.client[db_name]
        self.comparisons = self.db[collection_name]
        
    def test(self):
        print(self.comparisons.find_one())

    def save_comparison(self, event_id, date, comparison):
        comparison_record = {
            "event_id": event_id,
            "date": date,
            "comparison": comparison,
        }
        self.comparisons.insert_one(comparison_record)

    def get_comparison(self, event_id, date):
        return self.comparisons.find_one({"event_id": event_id, "date": date})