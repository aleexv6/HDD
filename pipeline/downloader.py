from ecmwf.opendata import Client

class Downloader:
    def __init__(self, request, download_dir):
        self.request = request 
        self.download_dir = download_dir

    def download(self):
        client = Client(source="ecmwf")
        data = client.retrieve(request=self.request, 
                               target=self.download_dir)
        return self.download_dir, data.datetime