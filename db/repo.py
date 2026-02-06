class ResultsRepository:
    def __init__(self, mongo_client, repo):
        self.col = mongo_client.collection(repo)

    def exists_for_date(self, date, name):
        return self.col.count_documents({"time": date, "source": name}) > 0

    def insert_results(self, df):
        records = df.to_dict('records')
        self.col.insert_many(records)