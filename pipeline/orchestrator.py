from datetime import timedelta
import pandas as pd

from pipeline.config import settings

from model.forecast import forecast_vs_forecast, base_vs_forecast, full_forecast

class PipelineOrchestrator:
    def __init__(self, downloader, repo):
        self.downloader = downloader
        self.repo = repo

    def run(self) -> bool:
        """
        Retourne True si un nouveau fichier a été traité
        False sinon
        """

        latest_run_date = self.downloader.check_latest_available()
        if self.repo.exists_for_date(pd.Timestamp(latest_run_date)):
            return False
        
        previous_run_date = pd.Timestamp(latest_run_date) - pd.Timedelta(hours=12)
        filepath = self.downloader.download()

        previous_run_data_list = self.repo.get_forecast(previous_run_date)
        if previous_run_data_list is None:
            return False

        horizons = [(latest_run_date.date() + timedelta(days=1), latest_run_date.date() + timedelta(days=3), 'Day 1-3'),
                    (latest_run_date.date() + timedelta(days=4), latest_run_date.date() + timedelta(days=7), 'Day 4-7'),
                    (latest_run_date.date() + timedelta(days=8), latest_run_date.date() + timedelta(days=14), 'Day 8-14')]

        base_forecast = base_vs_forecast(filepath, settings.BASE_FILE_PATH, horizons)
        forecast_forecast = forecast_vs_forecast(filepath, previous_run_data_list)
        df = full_forecast(base_forecast, forecast_forecast)

        self.repo.insert_results(df)
        
        return True