import pandas as pd
import logging

from pipeline.config import settings
from model.forecast import compute_forcast_hdd
from utils.tools import print_forecast

logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    def __init__(self, downloader, repo):
        self.downloader = downloader
        self.repo = repo

    def run(self) -> bool:
        latest_run_date = self.downloader.check_latest_available() #check for available to download run datetime
        if latest_run_date.hour != 0 and latest_run_date.hour != 12: #if run is not from 00z or 12z -> dont compute
            logger.info("Current run not from 00z or 12z, retrying...")
            return False
        if self.repo.exists_for_date(pd.Timestamp(latest_run_date)): #extra protection : if run is from 00z or 12z, we check if it exists in base, if yes -> don't compute
            logger.info("Current run already exists in database, retrying...")
            return False
        
        previous_run_date = pd.Timestamp(latest_run_date) - pd.Timedelta(hours=12) #compute previous run date from current run date
        filepath, dl_run_datetime = self.downloader.download()

        previous_run_data_list = self.repo.get_forecast(previous_run_date) #check in database if we have the previous forecast, if not -> can't compute
        if previous_run_data_list is None:
            logger.info("No previous forecast found in database, retrying...")
            return False

        #compute HDDs
        df = compute_forcast_hdd(filepath)

        self.repo.insert_results(df) #insert results in db

        #save img of current forecast
        print_forecast(settings.BASE_FILE_PATH, df, settings.IMG_OUTPUT_PATH)
        
        return True