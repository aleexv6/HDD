from dotenv import load_dotenv
import os

class settings:
    DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR")
    MONGO_URI = os.getenv("MONGO_URI")
    MONGO_DB = os.getenv("MONGO_DB")
