import xarray as xr
import pandas as pd

from utils.tools import regions_from_xarray

if __name__ == '__main__':
    #Read Zarr dataset from earthdatahub destine EU
    ds = xr.open_dataset(
        "https://data.earthdatahub.destine.eu/era5/era5-land-daily-utc-v1.zarr",
        storage_options={"client_kwargs":{"trust_env":True}},
        chunks={},
        engine="zarr",
    )

    #Format dataset for HDD
    t2m = ds.t2m #Keep only 2m temperature
    t2m = (t2m - 273.15) * 1.8 + 32 #Convert kelvin to °F
    t2m.attrs['units'] = '°F'
    t2m_30years = t2m.sel(valid_time=slice('1995', '2025')) #Select last 30 years

    #Reassign longitude to be -180, 180
    if t2m_30years.longitude.max() > 180:
        t2m_30years = t2m_30years.assign_coords(longitude=((t2m_30years.longitude + 180) % 360) - 180)
        t2m_30years = t2m_30years.sortby("longitude")
    t2m_30years_us = t2m_30years.sel(**{"latitude": slice(50, 24), "longitude": slice(-125, -67)}) #Slice to get only US 

    hdd = (65 - t2m_30years_us).clip(min=0) #make max(0, 65 - t2m)

    #Population reggrided to ERA5-Land dataset
    pop = xr.open_dataarray('../files/population_regridded_01deg.nc') #from reproject_and_align_pop function in tools

    hdd_weighted = hdd * pop #Weight the hdd by population for each point in the grid and each valid_time

    hdd_weighted_yearly_mean = hdd_weighted.groupby(hdd_weighted.valid_time.dt.dayofyear).mean() #Make the mean of each grid point for each day of year (1-365)
    hdd_weighted_yearly_mean_computed = hdd_weighted_yearly_mean.compute() #here we finally compute the xarray because that's the one we will then manipulate for US and subregion mean

    #Make US mean
    us_yearly_mean = hdd_weighted_yearly_mean_computed.mean(dim=['latitude', 'longitude']) #Mean every point in the US to have one mean weighted HDD for each day of year
    us_yearly_mean_df = us_yearly_mean.to_dataframe(name='US Mean')
    us_yearly_mean_df = us_yearly_mean_df[['US Mean']]

    #Make subregion mean
    zone_means = regions_from_xarray(hdd_weighted_yearly_mean_computed)

    #Make a dataframe from mean dict with day of year as index and regions as columns
    subregion_yearly_mean_df = pd.DataFrame({zone: data.values for zone, data in zone_means.items()}, index=hdd_weighted_yearly_mean_computed.dayofyear.values)
    us_base_hdd = pd.concat([us_yearly_mean_df, subregion_yearly_mean_df], axis=1) #add US Mean to a new column

    if 366 in us_base_hdd.index: #if 366 days in a year, we remove the last one (366th)
        us_base_hdd = us_base_hdd.drop(index=366)

    us_base_hdd.to_csv('utils/files/base.csv')