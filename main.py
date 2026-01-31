from dotenv import load_dotenv
load_dotenv()

from pipeline.downloader import Downloader
from pipeline.orchestrator import PipelineOrchestrator
from pipeline.runner import PipelineRunner
from db.repo import ResultsRepository
from db.mongo import MongoWrapper

from pipeline.config import settings

from datetime import datetime

def main():
    #Database connection
    mongo = MongoWrapper(settings.MONGO_URI, settings.MONGO_DB)

    steps = [i for i in range(0, 144, 3)] + [j for j in range(144, 361, 6)] #get all steps
    time = 0 if datetime.now().hour < 18 else 12 #get 00z forecast if we are before 18z, else get 12z forecast
    current_date = datetime.now().date() #select current date forecast
    var = '2t' #forecast returned variables

    request = {
        'date': current_date,
        'time': time,
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