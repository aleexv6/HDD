import time
import logging

from utils.tools import is_run_release_time

logger = logging.getLogger(__name__)

class PipelineRunner:
    def __init__(self, orchestrators, poll_interval=300):
        self.orchestrators = orchestrators
        self.poll_interval = poll_interval

    def run_forever(self):
        while True:
            if is_run_release_time():
                for orchestrator in self.orchestrators:
                    try:
                        processed = orchestrator.run()
                        if processed:
                            logger.info(f"Processed data for {orchestrator.downloader.name}")

                    except Exception as e:
                        logger.exception(f"Erreur pipeline on {orchestrator.downloader.name}")

            time.sleep(self.poll_interval)