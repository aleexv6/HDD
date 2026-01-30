class ResultsRepository:
    def __init__(self, mongo_client, repo):
        self.col = mongo_client.collection(repo)

    def exists_for_date(self, date):
        return self.col.count_documents({"date": date}) > 0

    def insert_results(self, date, file_name, df_a, df_b):
        pass