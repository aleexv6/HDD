class ResultsRepository:
    def __init__(self, mongo_client, repo):
        self.col = mongo_client.collection(repo)

    def exists_for_date(self, date):
        return self.col.count_documents({"time": date}) > 0

    def insert_results(self, df):
        records = df.to_dict('records')
        self.col.insert_many(records)

    def get_forecast(self, forecast_date):
        query_res = self.col.find({'time': forecast_date})
        return list(query_res)
