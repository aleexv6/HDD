from ecmwf.opendata import Client

class Downloader:
    def __init__(self, request, download_dir):
        self.request = request 
        self.download_dir = download_dir
        self.client = Client(source="ecmwf")

    def check_latest_available(self):
        latest_run_date = self.client.latest(
            step=0,
            type=self.request['time'],
            param=self.request['var'],
            target='None.grib2'
        )

        return latest_run_date

    def download(self):
        data = self.client.retrieve(request=self.request, 
                               target=self.download_dir)
        return self.download_dir