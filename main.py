from dotenv import load_dotenv
load_dotenv()

from pipeline.downloader import Downloader
from pipeline.orchestrator import PipelineOrchestrator
from pipeline.runner import PipelineRunner
from db.repo import ResultsRepository
from db.mongo import MongoWrapper
from utils.tools import setup_logging

from pipeline.config import settings

from datetime import datetime
import logging

def main():
    #Setup logging
    setup_logging(log_level=logging.INFO)
    
    #Database connection
    mongo = MongoWrapper(settings.MONGO_URI, settings.MONGO_DB)

    steps = [i for i in range(0, 144, 3)] + [j for j in range(144, 361, 6)] #get all steps
    var = '2t' #forecast returned variables

    request = {
        'type': "fc",
        'stream': 'oper',
        'step': steps,
        'param': var,
    }    

    downloader = Downloader(request=request, download_dir=settings.DOWNLOAD_DIR)
    repo = ResultsRepository(mongo_client=mongo, repo=settings.MONGO_COLLECTION)
    orchestrator = PipelineOrchestrator(downloader, repo)

    runner = PipelineRunner(
        orchestrator=orchestrator,
        poll_interval=300
    )

    runner.run_forever()

if __name__ == "__main__":
    main()