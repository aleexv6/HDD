import xarray as xr
import pandas as pd

from datetime import datetime

from utils.tools import regions_from_xarray

def compute_forcast_hdd(filepath):
    #Open the forecast dataset and format the datas
    ds = xr.open_dataset(filepath)
    forecast_date = ds.time.values #keep track of the forecast run date time
    us = ds.sel(**{"latitude": slice(50, 24), "longitude": slice(-125, -67)}) #Slice to get only US 
    us = (us - 273.15) * 1.8 + 32 #Convert kelvin to °F
    us.attrs['units'] = '°F'

    us = us.swap_dims({"step": "valid_time"})

    #We slice the dataset with valid_times to avoid non complete day between 00z run (complete days) and 12z run (non complete days)
    us = us.sel(valid_time=slice(pd.Timestamp(pd.Timestamp(ds.time.values).date()) + pd.Timedelta(days=1), #set the dataset at the start of the next day for our first window
                                pd.Timestamp(pd.Timestamp(ds.time.values).date()) + pd.Timedelta(days=14, hours=23))) #and just before the last valid_time

    #Now that we start the first day at 00z for 00z run AND 12z run, resample hourly to daily
    us_daily = us.resample(valid_time="1D").mean()

    us_daily_hdd = (65 - us_daily).clip(min=0) #compute HDD

    #Open population file reggrided to weather forecasts
    pop = xr.open_dataarray('../files/population_regridded_025deg.nc')

    hdd_weighted = us_daily_hdd * pop #Weight the hdd by population for each point in the grid and each valid_time

    #Compute HDD for every horizons
    horizons = [(0,2, 'Day 1-3'), (3,6, 'Day 4-7'), (7,13, 'Day 8-14')]
    hdd_list = []
    for horizon in horizons:
        #Create horizon date for slicing
        first_date = hdd_weighted.valid_time.min().values
        start_date = first_date + pd.Timedelta(days=horizon[0])
        last_date = first_date + pd.Timedelta(days=horizon[1])

        hdd_weighted_horizon = hdd_weighted.sel(valid_time=slice(start_date, last_date)).sum(dim='valid_time') #sumed HDD for every grid point in the horizon

        #Make US mean
        us_horizon_mean = hdd_weighted_horizon.mean(dim=['latitude', 'longitude']) #Mean every point in the US to have one mean weighted HDD for the horizon
        hdd_list.append({'forecast_run_time': forecast_date, 'region': 'US Mean', 'horizon_start': start_date, 'horizon_end': last_date,
                        'horizon_label': horizon[2], 'forecast_HDD': us_horizon_mean.t2m.values})
        
        #Make region means
        zone_means = regions_from_xarray(hdd_weighted_horizon)

        #For each zone in the horizon, make a new row of data
        for zone_name, zone_data in zone_means.items():
            hdd_list.append({'forecast_run_time': forecast_date, 'region': zone_name, 'horizon_start': start_date, 'horizon_end': last_date,
                            'horizon_label': horizon[2], 'forecast_HDD': zone_data.t2m.values})
            
    #Make a dataframe of values
    hdd_horizon_df = pd.DataFrame(hdd_list)

    return hdd_horizon_df

def base_vs_forecast(filepath, base_filepath):
    pass

def forecast_vs_forecast(filepath, last_forecast):
    pass