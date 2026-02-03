import geopandas as gpd
import numpy as np
import pandas as pd
import xarray as xr

from datetime import datetime, timezone
import logging
import sys
from logging.handlers import RotatingFileHandler

def regions_from_xarray(dataset):
    #Load subregion shapefile
    gdf = gpd.read_file('utils/files/USRegion/12Regions/DOI_12_Unified_Regions_20180801.shp')
    gdf = gdf.to_crs(epsg=4326) #reproject file into classical lat lon coordinates
    gdf = gdf[['REG_NAME', 'geometry']]
    gdf = gdf[~gdf['REG_NAME'].isin(['Alaska', 'Pacific Islands'])] #Remove both of these regions, irrelevant

    #Make a point grid from dataset
    lon_grid, lat_grid = np.meshgrid(dataset.longitude.values, dataset.latitude.values)
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
        zone_data = dataset.sel(latitude=xr.DataArray(zone_pts['latitude'].values, dims='points'), #Select region points from the xarray dataset
                                                        longitude=xr.DataArray(zone_pts['longitude'].values, dims='points'),
                                                        method='nearest')
        zone_means[zone] = zone_data.mean(dim='points') #mean the founded points and store in the dict

    return zone_means

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