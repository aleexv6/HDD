import time
import logging

logger = logging.getLogger(__name__)

class PipelineRunner:
    def __init__(self, orchestrator, poll_interval=300):
        self.orchestrator = orchestrator
        self.poll_interval = poll_interval

    def run_forever(self):
        while True:
            try:
                processed = self.orchestrator.run()
                if processed:
                    logger.info("Processed data")

            except Exception as e:
                logger.exception("Erreur pipeline")

            time.sleep(self.poll_interval)