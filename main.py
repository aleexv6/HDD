from dotenv import load_dotenv
load_dotenv()

from pipeline.downloader import ECMWFDownloader, ERA5LandDownloader
from pipeline.orchestrator import PipelineOrchestrator
from pipeline.runner import PipelineRunner
from db.repo import ResultsRepository
from db.mongo import MongoWrapper
from utils.tools import setup_logging

from pipeline.config import settings

import logging

def main():
    #Setup logging
    setup_logging(log_level=logging.INFO)
    
    steps = [i for i in range(0, 144, 3)] + [j for j in range(144, 361, 6)] #get all steps
    var = '2t' #temperature at 2m
    request_ecmwf = {
        'type': "fc",
        'stream': 'oper',
        'step': steps,
        'param': var,
    }    

    request_era5 = {
        "variable": ["2m_temperature"],
        "time": [
            "00:00", "01:00", "02:00",
            "03:00", "04:00", "05:00",
            "06:00", "07:00", "08:00",
            "09:00", "10:00", "11:00",
            "12:00", "13:00", "14:00",
            "15:00", "16:00", "17:00",
            "18:00", "19:00", "20:00",
            "21:00", "22:00", "23:00"
        ],
        "data_format": "grib",
        "download_format": "unarchived",
        "area": [50, -125, 24, -67]
    }

    ecmwf_downloader = ECMWFDownloader( #forecast downloader
        request=request_ecmwf,
        download_dir=settings.DOWNLOAD_ECMWF_PATH
    )
    era5_downloader = ERA5LandDownloader( #observation downloader
        request=request_era5,
        download_dir=settings.DOWNLOAD_ERA5_PATH
    ) 
    mongo = MongoWrapper(settings.MONGO_URI, settings.MONGO_DB) #mongo client
    repo = ResultsRepository(mongo_client=mongo, repo=settings.MONGO_COLLECTION) #mongo repo
    orchestrators = [ #orchestrators for gestion for the two downloaders
        PipelineOrchestrator(ecmwf_downloader, repo),
        PipelineOrchestrator(era5_downloader, repo),
    ]

    runner = PipelineRunner( #running pipeline obj
        orchestrators=orchestrators,
        poll_interval=300
    )

    runner.run_forever()

if __name__ == "__main__":
    main()