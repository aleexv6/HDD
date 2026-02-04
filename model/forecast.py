import xarray as xr
import pandas as pd

from utils.tools import regions_from_xarray

def compute_forcast_hdd(filepath, horizons):
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

    #Compute HDD for every horizons
    hdd_list = []
    for horizon in horizons:
        #Create horizon date for slicing
        start_date = min(horizon[0:2])
        last_date = max(horizon[0:2])

        hdd_weighted_horizon = hdd_weighted.sel(valid_time=slice(start_date, last_date)).sum(dim='valid_time') #sumed HDD for every grid point in the horizon

        #Make US sum
        us_horizon_sum = hdd_weighted_horizon.sum(dim=['latitude', 'longitude']) #Sum every HDD point in the US to have one weighted HDD for the horizon
        base_us_weighted_hdd = us_horizon_sum / us_pop_sum

        hdd_list.append({'forecast_run_time': forecast_date, 'region': 'US Mean', 'horizon_start': pd.Timestamp(start_date), 'horizon_end': pd.Timestamp(last_date),
                        'horizon_label': horizon[2], 'forecast_HDD': base_us_weighted_hdd.t2m.item()})
        
        #Make region sums
        zone_means = regions_from_xarray(hdd_weighted_horizon, pop)

        #For each zone in the horizon, make a new row of data
        for zone_name, zone_data in zone_means.items():
            hdd_list.append({'forecast_run_time': forecast_date, 'region': zone_name, 'horizon_start': pd.Timestamp(start_date), 'horizon_end': pd.Timestamp(last_date),
                            'horizon_label': horizon[2], 'forecast_HDD': zone_data.t2m.item()})
            
    #Make a dataframe of values
    hdd_horizon_df = pd.DataFrame(hdd_list)

    return hdd_horizon_df

def base_vs_forecast(filepath, base_filepath, horizons):
    #Load and format base HDD file
    us_base_hdd = pd.read_csv(base_filepath)
    us_base_hdd = us_base_hdd.rename(columns={'Unnamed: 0': 'doy'})
    us_base_hdd = us_base_hdd.set_index('doy')

    #Compute HDD from forecast file
    hdd_forecast = compute_forcast_hdd(filepath, horizons)

    #Add day of year columns to prepare the sum of base days
    hdd_forecast['doy_horizon_start'] = hdd_forecast['horizon_start'].dt.day_of_year
    hdd_forecast['doy_horizon_end'] = hdd_forecast['horizon_end'].dt.day_of_year

    #Prepare a list of horizon days with horizon label
    horizon_doy = list(set([(start, end, label) for start, end, label in zip(hdd_forecast['doy_horizon_start'].values, 
                                                                             hdd_forecast['doy_horizon_end'].values, 
                                                                             hdd_forecast['horizon_label'].values)]))
    
    #Sum the mean base hdd for each day in each horzion and each region
    hdd_horizon_list = []
    for hd in horizon_doy:
        us_base_hdd_horizon = us_base_hdd.loc[hd[0]:hd[1]]
        summed_base_hdd_horizon = pd.DataFrame(us_base_hdd_horizon.T.sum(axis=1).reset_index())
        summed_base_hdd_horizon = summed_base_hdd_horizon.rename(columns={'index': 'region', 0: 'sum_base_HDD'})
        summed_base_hdd_horizon['horizon_label'] = hd[2]
        hdd_horizon_list.append(summed_base_hdd_horizon)
    full_hdd_base_horizon = pd.concat(hdd_horizon_list)

    #Merge the sum base hdd and the forcast hdd on each region and each horizon
    hdd_forecast_base = pd.merge(hdd_forecast, full_hdd_base_horizon, how='inner', on=['region', 'horizon_label'])
    hdd_forecast_base['delta_forecast_base'] = hdd_forecast_base['forecast_HDD'] - hdd_forecast_base['sum_base_HDD'] #delta between forecast and base

    return hdd_forecast_base

def forecast_vs_forecast(current_run_filepath, previous_forecast):
        #Get previous forecast as df and set prev horizon
        previous_hdd = pd.DataFrame(previous_forecast)
        if not previous_hdd.empty:
            previous_hdd = previous_hdd.drop('_id', axis=1)
            prev_forecast_horizon = list(set([(pd.Timestamp(start), pd.Timestamp(end), label) for start, end, label in zip(previous_hdd['horizon_start'].values, 
                                                                                    previous_hdd['horizon_end'].values, 
                                                                                    previous_hdd['horizon_label'].values)]))
            
        #Compute current forecast file with previous forecast time horizon
        current_hdd_with_prev_horizon_df = compute_forcast_hdd(current_run_filepath, prev_forecast_horizon)

        #Make sure that we have the same number of rows in both previous and current forecasts
        assert len(previous_hdd) == len(current_hdd_with_prev_horizon_df)

        #Format and merge and compute current - previous forecast
        previous_hdd = previous_hdd.rename(columns={'forecast_run_time': 'prev_forecast_run_time', 'forecast_HDD': 'prev_forecast_HDD'})
        current_and_prev_hdd = pd.merge(previous_hdd, current_hdd_with_prev_horizon_df, how='inner', on=['region', 'horizon_start', 'horizon_end', 'horizon_label'])
        current_and_prev_hdd['delta_current_forecast_to_prev_forecast'] = current_and_prev_hdd['forecast_HDD'] - current_and_prev_hdd['prev_forecast_HDD']
        delta_forecast_to_forecast = current_and_prev_hdd[['forecast_run_time', 'region', 'horizon_start', 'horizon_end', 'horizon_label', 'delta_current_forecast_to_prev_forecast']]
        delta_forecast_to_forecast = delta_forecast_to_forecast.rename(columns={'horizon_start': 'prev_horizon_start', 'horizon_end': 'prev_horizon_end'})

        return delta_forecast_to_forecast


def full_forecast(base_forecast_data, forecast_vs_forecast_data):
    full_hdd = pd.merge(base_forecast_data, forecast_vs_forecast_data, how='inner', on=['forecast_run_time', 'region', 'horizon_label']).reset_index(drop=True)
    return full_hdd