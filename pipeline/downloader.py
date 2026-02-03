from ecmwf.opendata import Client

class Downloader:
    def __init__(self, request, download_dir=None):
        self.request = request 
        self.download_dir = download_dir
        self.client = Client(source="ecmwf")

    def check_latest_available(self):
        latest_run_date = self.client.latest(request=self.request, 
                                             target='None.grib2')
        return latest_run_date

    def download(self):
        data = self.client.retrieve(request=self.request, 
                               target=self.download_dir)
        return self.download_dir, data.datetime