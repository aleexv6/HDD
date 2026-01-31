import time
import logging

class PipelineRunner:
    def __init__(self, orchestrator, poll_interval=300):
        self.orchestrator = orchestrator
        self.poll_interval = poll_interval

    def run_forever(self):
        while True:
            try:
                processed = self.orchestrator.run()
                if processed:
                    logging.info("Processed data")
                else:
                    logging.info("Not processed")

            except Exception as e:
                logging.exception("Erreur pipeline")

            time.sleep(self.poll_interval)