class PipelineOrchestrator:
    def __init__(self, downloader, repo):
        self.downloader = downloader
        self.repo = repo

    def run(self) -> bool:
        """
        Retourne True si un nouveau fichier a été traité
        False sinon
        """
        filepath, data_date = self.downloader.download()

        if self.repo.exists_for_date(data_date):
            return False

        base_forecast = base_vs_forecast(filepath)
        forecast_forecast = forecast_vs_forecast(filepath)

        self.repo.insert_results()
        
        return True