import pandas as pd
import logging

logger = logging.getLogger(__name__)

class PipelineOrchestrator:
    def __init__(self, downloader, repo):
        self.downloader = downloader
        self.repo = repo

    def run(self):
        latest_date = pd.Timestamp(self.downloader.check_latest_available()) #check for available to download run datetime
        
        #if we have an ecmwf downloader, check if the run is 0 or 12, if not, we do not compute
        if not self.downloader.is_valid_run(latest_date):
            return False
        
        if self.repo.exists_for_date(latest_date, self.downloader.name): #extra protection : if it exists in base, don't compute
            logger.info("Current run already exists in database, retrying...")
            return False
        
        filepath, dt = self.downloader.download(latest_date)

        #compute HDDs
        df = self.downloader.compute(filepath, latest_date)

        self.repo.insert_results(df) #insert results in db
       
        return True