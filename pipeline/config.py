import os

class settings:
    DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR")
    MONGO_URI = os.getenv("MONGO_URI")
    MONGO_DB = os.getenv("MONGO_DB")
    MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")
    BASE_FILE_PATH = os.getenv("BASE_FILE_PATH")
    IMG_OUTPUT_PATH = os.getenv("IMG_OUTPUT_PATH")