import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.pyplot as plt

import math
from datetime import datetime, timezone
import logging
import sys
from logging.handlers import RotatingFileHandler

def regions_from_xarray(hdd_dataset, pop_dataset):
    #Load subregion shapefile
    gdf = gpd.read_file('utils/files/USRegion/12Regions/DOI_12_Unified_Regions_20180801.shp')
    gdf = gdf.to_crs(epsg=4326) #reproject file into classical lat lon coordinates
    gdf = gdf[['REG_NAME', 'geometry']]
    gdf = gdf[~gdf['REG_NAME'].isin(['Alaska', 'Pacific Islands'])] #Remove both of these regions, irrelevant

    #Make a point grid from dataset
    lon_grid, lat_grid = np.meshgrid(hdd_dataset.longitude.values, hdd_dataset.latitude.values)
    coords_flat = list(zip(lon_grid.ravel(), lat_grid.ravel()))

    #Make a gdf of dataset grid to prepare for spatial join
    gdf_points = gpd.GeoDataFrame({
        'latitude': lat_grid.flatten(),
        'longitude': lon_grid.flatten(),
        'geometry': gpd.points_from_xy([c[0] for c in coords_flat], [c[1] for c in coords_flat])
    }, crs=gdf.crs) #Make sure we have the same crs

    #Spatial join between the full US grid and the subregions
    gdf_joined = gpd.sjoin(gdf_points, gdf[['REG_NAME', 'geometry']], how='inner', predicate='within')

    #Find subregions points and mean then to have the mean of a region for a day of year
    zone_means = {}
    for zone in gdf_joined['REG_NAME'].unique():
        zone_pts = gdf_joined[gdf_joined['REG_NAME'] == zone]
        zone_data = hdd_dataset.sel(latitude=xr.DataArray(zone_pts['latitude'].values, dims='points'), #Select region points from the xarray dataset
                                    longitude=xr.DataArray(zone_pts['longitude'].values, dims='points'),
                                    method='nearest')
        zone_pop = pop_dataset.sel(latitude=xr.DataArray(zone_pts['latitude'].values, dims='points'), #Select region points from the xarray dataset
                                   longitude=xr.DataArray(zone_pts['longitude'].values, dims='points'),
                                   method='nearest')
        zone_hdd_sum = zone_data.sum(dim='points') #sum the founded points HDD
        zone_pop_sum = zone_pop.sum(dim='points') #sum the pop

        zone_hdd_sum_pop_weighted = zone_hdd_sum / zone_pop_sum
        
        zone_means[zone] = zone_hdd_sum_pop_weighted

    return zone_means

def print_forecast(base_filepath, current_forecast_hdd, output_filepath):
    current_forecast_hdd['valid_time_doy'] = current_forecast_hdd['valid_time'].dt.dayofyear #find day of year of every run

    #open and format hist hdd
    base_hdd = pd.read_csv(base_filepath)
    base_hdd = base_hdd.rename(columns={'Unnamed: 0': 'Day of year'})
    base_hdd = base_hdd.set_index('Day of year')

    base_hdd_rearranged = pd.concat([base_hdd.loc[200:365], base_hdd.loc[0:199]]) #rearrange the index for a better plot

    cols = 4
    rows = math.ceil(len(base_hdd_rearranged.columns) / cols)

    fig, ax = plt.subplots(nrows=rows, ncols=cols, figsize=(14, 2.5 * rows))
    ax = ax.flatten()

    tick_positions = range(25, len(base_hdd_rearranged), 50)  #Every 50 days
    tick_labels = [base_hdd_rearranged.index[pos] for pos in tick_positions]

    for i, col in enumerate(base_hdd_rearranged.columns):
        current_hdd_region = current_forecast_hdd[current_forecast_hdd['region'] == col]
        #transform doy values to match rearranged index
        transformed_doy = current_hdd_region['valid_time_doy'].apply(
            lambda x: x - 200 if x >= 200 else x + 166  # 366-200=166
        )
        ax[i].plot(base_hdd_rearranged[col].values, linewidth=2, label='30y average')
        ax[i].scatter(transformed_doy, current_hdd_region['hdd'], s=15, alpha=0.5, color='green', label='14days forecast')
        ax[i].set_title(f'{col}')
        ax[i].set_xticks(tick_positions)
        ax[i].set_xticklabels(tick_labels)
        ax[i].set_xlabel('Day of year')
        ax[i].set_ylabel('Pop. weighted HDD')
        ax[i].legend(fontsize=6)
    plt.suptitle(f'Population weighted HDD average and forecast as of {current_forecast_hdd['time'].unique()[0]}')
    plt.tight_layout()
    plt.savefig(output_filepath)

def setup_logging(log_level=logging.INFO, log_file=None):
    #Reset existing config
    logging.shutdown()
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    #Log format
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    #Handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    handlers = [console_handler]

    #If file log in file
    if log_file:
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10*1024*1024, backupCount=5, encoding="utf-8"
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    logging.basicConfig(
        level=log_level,
        handlers=handlers,
    )

def is_run_release_time():
    now = datetime.now(timezone.utc)
    minutes = now.hour * 60 + now.minute

    #00z window: 6am to 9am UTC
    if 6 * 60 <= minutes < 9 * 60:
        return True

    #00z window: 6pm to 9pm UTC
    if 18 * 60 <= minutes < 21 * 60:
        return True

    return False