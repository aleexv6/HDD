from pipeline.compute_hdd import compute_forecast_hdd, compute_observation_hdd

from ecmwf.opendata import Client
import cdsapi
import requests

class WeatherDownloader:
    name: str

    def check_latest_available(self):
        raise NotImplementedError
    
    def is_valid_run(self, latest_date):
        return True

    def download(self, date):
        raise NotImplementedError

    def compute(self, filepath, latest_date):
        raise NotImplementedError

class ECMWFDownloader(WeatherDownloader):
    name = "ecmwf"

    def __init__(self, request, download_dir=None):
        self.request = request 
        self.download_dir = download_dir
        self.client = Client(source="ecmwf")

    def check_latest_available(self):
        latest_run_date = self.client.latest(request=self.request, 
                                             target='None.grib2')
        return latest_run_date
    
    def is_valid_run(self, latest_date):
        return latest_date.hour in (0, 12)

    def download(self, date):
        data = self.client.retrieve(request=self.request, 
                               target=self.download_dir)
        return self.download_dir, data.datetime
    
    def compute(self, filepath, latest_date):
        return compute_forecast_hdd(filepath, latest_date)
    
class ERA5LandDownloader(WeatherDownloader):
    name = "era5_land"

    def __init__(self, request, download_dir=None):
        self.request = request 
        self.download_dir = download_dir
        self.client = cdsapi.Client()

    def check_latest_available(self):
        metadata_url = 'https://cds.climate.copernicus.eu/api/catalogue/v1/collections/reanalysis-era5-land'
        response = requests.get(metadata_url)
        temporal_extent = response.json()['extent']['temporal']['interval'][0]
        start_date = temporal_extent[0]
        last_date = temporal_extent[1]
        return last_date
    
    def is_valid_run(self, latest_date):
        return True

    def download(self, date):
        self.request['year'] = str(date.year)
        self.request['month'] = f"{date.month:02d}"
        self.request['day'] = [f"{date.day:02d}"]
        self.client.retrieve("reanalysis-era5-land", self.request, self.download_dir)
        return self.download_dir, None

    def compute(self, filepath, latest_date):
        return compute_observation_hdd(filepath, latest_date)