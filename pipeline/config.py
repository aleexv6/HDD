import os

class settings:
    DOWNLOAD_ECMWF_PATH = os.getenv("DOWNLOAD_ECMWF_PATH")
    DOWNLOAD_ERA5_PATH = os.getenv("DOWNLOAD_ERA5_PATH")

    MONGO_URI = os.getenv("MONGO_URI")
    MONGO_URI_PROD = os.getenv("MONGO_URI_PROD")
    MONGO_DB = os.getenv("MONGO_DB")
    MONGO_COLLECTION = os.getenv("MONGO_COLLECTION")

    BASE_FILE_PATH = os.getenv("BASE_FILE_PATH")
    IMG_OUTPUT_PATH = os.getenv("IMG_OUTPUT_PATH")