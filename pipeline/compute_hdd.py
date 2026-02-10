import xarray as xr
import pandas as pd

from utils.tools import regions_from_xarray

def compute_forecast_hdd(filepath, latest_date):
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
    pop = xr.open_dataarray('utils/files/population_regridded_025deg.nc')
    us_pop_sum = pop.sum(dim=['latitude', 'longitude'])

    hdd_weighted = us_daily_hdd * pop #Weight the hdd by population for each point in the grid and each valid_time

    #Make US sum and format a df
    hdd_weighted_sum = hdd_weighted.sum(dim=['latitude', 'longitude']) #Sum every HDD point in the US to have one weighted HDD
    hdd_weighted_sum_per_pop = hdd_weighted_sum / us_pop_sum
    hdd_weighted_sum_per_pop_df = hdd_weighted_sum_per_pop.to_dataframe().reset_index()
    hdd_weighted_sum_per_pop_df = hdd_weighted_sum_per_pop_df[['time', 'valid_time', 't2m']]
    hdd_weighted_sum_per_pop_df['time'] = forecast_date
    hdd_weighted_sum_per_pop_df['region'] = 'US Sum'
    hdd_weighted_sum_per_pop_df = hdd_weighted_sum_per_pop_df.rename(columns={'t2m': 'hdd'})

    #Make regions sum and format a df
    region_list_hdd = []
    zone_means = regions_from_xarray(hdd_weighted, pop)
    for zone_name, zone_data in zone_means.items():
        hdd_weighted_sum_per_pop_region_df = zone_data.to_dataframe().reset_index()
        hdd_weighted_sum_per_pop_region_df = hdd_weighted_sum_per_pop_region_df[['time', 'valid_time', 't2m']]
        hdd_weighted_sum_per_pop_region_df['time'] = forecast_date
        hdd_weighted_sum_per_pop_region_df['region'] = zone_name
        hdd_weighted_sum_per_pop_region_df = hdd_weighted_sum_per_pop_region_df.rename(columns={'t2m': 'hdd'})
        region_list_hdd.append(hdd_weighted_sum_per_pop_region_df)
    regions_hdd = pd.concat(region_list_hdd)

    total_hdds = pd.concat([hdd_weighted_sum_per_pop_df, regions_hdd]).reset_index(drop=True) #concat US and regions into a single df

    total_hdds['source'] = 'ecmwf'
    total_hdds['data_type'] = 'forecast'
    total_hdds['winter_year'] = total_hdds['valid_time'].dt.to_period('Y-MAY').dt.start_time.dt.year

    return total_hdds

def compute_observation_hdd(filepath, latest_date):
    ds = xr.open_dataset(filepath)
    ds = ds.sel(time=latest_date.tz_localize(None))
    observation_date = ds.time.values
    ds = (ds - 273.15) * 1.8 + 32 #Convert kelvin to °F
    ds.attrs['units'] = '°F'

    ds = ds.swap_dims({"step": "valid_time"})

    ds_daily = ds.resample(valid_time="1D").mean()
    ds_daily = ds_daily.sel(valid_time=latest_date.tz_localize(None))

    ds_daily_hdd = (65 - ds_daily).clip(min=0) #compute HDD

    #Open population file reggrided to weather forecasts
    pop = xr.open_dataarray('utils/files/population_regridded_01deg_era5land_hourly.nc')
    us_pop_sum = pop.sum(dim=['latitude', 'longitude'])

    hdd_weighted = ds_daily_hdd * pop #Weight the hdd by population for each point in the grid and each valid_time

    #Make US sum and format a df
    hdd_weighted_sum = hdd_weighted.sum(dim=['latitude', 'longitude']) #Sum every HDD point in the US to have one weighted HDD for the horizon
    hdd_weighted_sum_per_pop = hdd_weighted_sum / us_pop_sum
    hdd_weighted_sum_per_pop_df = pd.DataFrame([{'time': observation_date,
                                                 'region': 'US Sum', 
                                                 'hdd': hdd_weighted_sum_per_pop.t2m.values.item()}])
    
    #Make regions sum and format a df
    region_list_hdd = []
    zone_means = regions_from_xarray(hdd_weighted, pop)
    for zone_name, zone_date in zone_means.items():
        region_list_hdd.append({'time': observation_date,
                                'region': zone_name, 
                                'hdd': zone_date.t2m.values.item()})
    hdd_weighted_sum_per_pop_region_df = pd.DataFrame(region_list_hdd)

    total_hdds = pd.concat([hdd_weighted_sum_per_pop_df, hdd_weighted_sum_per_pop_region_df]).reset_index(drop=True) #concat US and regions into a single df

    total_hdds['source'] = 'era5_land'
    total_hdds['data_type'] = 'observation'
    total_hdds['winter_year'] = total_hdds['time'].dt.to_period('Y-MAY').dt.start_time.dt.year

    return total_hdds