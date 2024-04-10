import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import glob
import os
import merge_sensor_data
import xarray as xr
from functools import reduce

# %%
dir_in = r"C:\Users\le\OneDrive - Ilmatieteen laitos\WMO-DC\Jokioinen/"

# %%
sub_dir = [f.path for f in os.scandir(dir_in) if f.is_dir()]
subsub_dir = [f.path for dirdir_in in sub_dir for f in os.scandir(dirdir_in)  if f.is_dir() and 'calibration' not in f.path]
save_path = sub_dir[-1] + '/'
paths = [x + '/' for x in subsub_dir[:-1]]

# %%
# Define meta data
operatorID = '050'
airframeID = 'mavic3'
platform_name = 'Mavic 3'
flight_id = 'Jokioinen_1000m_VP'

# %%
for path in paths:
    rename_dict = {
        'latitude' : 'lat',
        'longitude' : 'lon',
        'altitude' : 'altitude',
        'temp_sht': 'air_temperature',
        'rh_sht' : 'relative_humidity',
        'press_bme' : 'pressure',
    }
    
    file_type = '*.txt'
    all_files = glob.glob(path + file_type)
    
    for file in all_files:
        if 'SHT_BP4' not in file:
            rename_dict = {
                'latitude' : 'lat',
                'longitude' : 'lon',
                'altitude' : 'altitude',
                'temp_bme': 'air_temperature',
                'rh_bme' : 'relative_humidity',
                'press_bme' : 'pressure',
            }
    
    data = []
    for file in all_files:
        df = pd.read_csv(file, sep=None, engine='python')
        df['datetime'] = pd.to_datetime(df['date'] + ' '+ df['time'])
        df = df.drop(['date', 'time'], axis=1)
        data.append(df)
    data_merged = reduce(lambda left, right: pd.merge(left, right,
                                                      on='datetime',
                                                      how='inner'),
                         data)
    pd_time = data_merged['datetime']
    
    data_merged = data_merged.rename(rename_dict, axis=1)
    data_merged = data_merged[[v for _, v in rename_dict.items()]]
    
    ds = xr.Dataset.from_dataframe(data_merged)
    ds = ds.rename({'index':'obs'})
    ref_time = np.datetime64('1970-01-01T00:00:00')
    ds['time'] = (pd_time.values - ref_time) / np.timedelta64(1, 's')
    
    # Convert Air Temperature from Celsius to Kelvin
    ds['air_temperature'] = ds['air_temperature'] + 273.15
    
    # Adding a REQUIRED Variable to the Dataset 
    ds['humidity_mixing_ratio'] = np.nan
    
    # Adding attributes to variables in the xarray dataset
    ds['time'].attrs = {'units': 'seconds since 1970-01-01T00:00:00', 'long_name': 'Time', '_FillValue': float('nan'), 'processing_level': ''}
    ds['lat'].attrs = {'units': 'degrees_north', 'long_name': 'Latitude', '_FillValue': float('nan'), 'processing_level': ''}
    ds['lon'].attrs = {'units': 'degrees_east', 'long_name': 'Longitude', '_FillValue': float('nan'), 'processing_level': ''}
    ds['altitude'].attrs = {'units': 'meters', 'long_name': 'Altitude', '_FillValue': float('nan'), 'processing_level': ''}
    ds['air_temperature'].attrs = {'units': 'Kelvin', 'long_name': 'Air Temperature', '_FillValue': float('nan'), 'processing_level': ''}
    ds['humidity_mixing_ratio'].attrs = {'units': 'kg/kg', 'long_name': 'Humidity Mixing Ratio', '_FillValue': float('nan'), 'processing_level': ''}
    ds['relative_humidity'].attrs = {'units': '%', 'long_name': 'Relative Humidity', '_FillValue': float('nan'), 'processing_level': ''}
    ds['pressure'].attrs = {'units': 'Pa', 'long_name': 'Atmospheric Pressure', '_FillValue': float('nan'), 'processing_level': ''}
        
    # Add Global Attributes
    ds.attrs['Conventions'] = "CF-1.8, WMO-CF-1.0"
    ds.attrs['wmo__cf_profile'] = "GS_wxhive_admin"
    ds.attrs['featureType'] = "vertical_profile"
    ds.attrs['platform_name'] = platform_name # example "GS_weatherhive"
    ds.attrs['flight_id'] = flight_id # example: "JBCC_1500m_VP"
    ds.attrs['processing_level'] = "raw"
    
    # Grab Initial timestamp of observations
    timestamp_dt = pd.to_datetime(ds['time'].values[0], unit='s', origin='unix')
    
    # Format datetime object to desired format (YYYYMMDDHHMMSSZ)
    formatted_timestamp = timestamp_dt.strftime('%Y%m%d%H%M%S') + 'Z'
    
    # Save to a NetCDF fileR
    ds.to_netcdf(save_path + f'UASDC_{operatorID}_{airframeID}_{formatted_timestamp}.nc')
    