# coding: utf-8

import xarray as xr
import numpy as np
import pandas as pd
from glob import glob
import sys
from datetime import datetime, timedelta
#import matplotlib.pyplot as plt            # Module to produce figureimport matplotlib.colors as colors
#import cartopy.crs as ccrs                 # Import cartopy ccrs
#import cartopy.feature as cfeature         # Import cartopy common features
#import matplotlib.colors


def main():

  sim = "CTRL"
  st = f"/chinook/marinier/CONUS_2D/{sim}"

  datai = 2001
  dataf = 2013  

  store = '/chinook/cruman/Data/WetSnow' 

  city = ["Edmundston", "Bathurst", "Miramichi", "Moncton", "Fredericton", "Saint John"] 
  city_lat = [47.42, 47.63, 47.01, 46.11, 45.87, 45.32]
  city_lon = [-68.32, -65.74, -65.47, -64.68, -66.53, -65.89]

  events_wsn = np.zeros([6,6])
  events_sn = np.zeros([6,6])
  events_pr = np.zeros([6,6])

  '''
 Start parameters: 
 City, initial date, final date 
 Get the location of the city. Use it and the grids around it in the calculations. 
 For each month, get the continuous events of snow, sum it. Add it to the bin: [10, 20, 30, 40, 50, 50+] 
 Make the plot at the end, similar to this one:  
  '''

  # annual data
  for y in range(datai, dataf+1):
    print(f"Year: {y}")

    for m in range(1, 13):

      # Open Dataset
      if 1 <= m <= 3:
        mi = 1
        mf = 3
      elif 4 <= m <= 6:
        mi = 4
        mf = 6
      elif 7 <= m <= 9:
        mi = 7
        mf = 9
      else:
        mi = 10
        mf = 12

      wsn = xr.open_dataset(f'{store}/{y}/WetSnow_SN_{y}{mi:02d}-{y}{mf:02d}.nc', engine='netcdf4')
          
      sn = xr.open_dataset(f'{st}/{y}/wrf2d_d01_CTRL_SNOW_ACC_NC_{y}{mi:02d}-{y}{mf:02d}.nc', engine='netcdf4')
      uu = xr.open_dataset(f'{st}/{y}/wrf2d_d01_CTRL_EU10_{y}{mi:02d}-{y}{mf:02d}.nc', engine='netcdf4')
      vv = xr.open_dataset(f'{st}/{y}/wrf2d_d01_CTRL_EV10_{y}{mi:02d}-{y}{mf:02d}.nc', engine='netcdf4')
      pr = xr.open_dataset(f'{st}/{y}/wrf2d_d01_CTRL_PREC_ACC_NC_{y}{mi:02d}-{y}{mf:02d}.nc', engine='netcdf4')

      # Slicing the domain to make the computations faster
      i1=721; j1=1167; i2=874; j2=1333
      
      sn = sn.SNOW_ACC_NC
      sn = sn[:,i1:i2,j1:j2] 
        
      uu = uu.EU10
      uu = uu[:,i1:i2,j1:j2]
        
      vv = vv.EV10
      vv = vv[:,i1:i2,j1:j2]

      pr_lat = pr.XLAT
      pr_lon = pr.XLONG

      pr = pr.PREC_ACC_NC
      pr = pr[:,i1:i2,j1:j2]      

      print(f"Month: {m}")

      datai = datetime.strptime(f'{y}-{m:02d}-01 00:00', '%Y-%m-%d %H:%M')
      dataf = datetime.strptime(f'{y}-{m:02d}-01 00:00', '%Y-%m-%d %H:%M') + timedelta(month=1)
      
      wsn = wsn.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      sn = sn.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      uu = uu.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      vv = vv.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      pr = pr.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))      

      for k, coords in enumerate(zip(city_lat, city_lon)):

        lat = coords[0]
        lon = coords[1]

        i, j = geo_idx([lat, lon], np.array([wsn.XLAT, wsn.XLONG]))

        ii, jj = geo_idx([lat, lon], np.array([pr_lat, pr_lon]))

        aux_wsn = wsn[:,i,j]
        aux_sn = sn[:,ii,jj]
        aux_uu = uu[:,ii,jj]
        aux_vv = vv[:,ii,jj]
        aux_pr = pr[:,ii,jj]

        events_wsn[k] = getEvents(aux_wsn, events_wsn[k])    
        events_sn[k] = getEvents(aux_sn, events_sn[k])
        events_pr[k] = getEvents(aux_pr, events_pr[k])

        # 0-10, 10-20, 20-30, 30-40, 40-50, 50+
        events_limits = [10,20,30,40,50,60]
    
    print(events_wsn)
    print(events_sn)
    print(events_pr)
    sys.exit()


def getEvents(data, events):

  # 0-10, 10-20, 20-30, 30-40, 40-50, 50+
  aux = 0

  for k in range(len(aux_wsn)):
    item = data[k]

    # Check each item in the array. While not 0, add to aux. When 0, store aux, set it to 0. Get the max wind between the start and end.
    if item == 0 and aux == 0:
      continue
    elif item == 0:
      # Streak ended
      # get max wind
      # for later
      # store aux
      index = int(aux/10)
      if index > 6:
        index = 6

      events[index] += 1
      # reset aux
      aux = 0
    else:
      aux += item

  return events

             
          
def geo_idx(dd, dd_array, type="lat"):
  '''
    search for nearest decimal degree in an array of decimal degrees and return the index.
    np.argmin returns the indices of minium value along an axis.
    so subtract dd from all values in dd_array, take absolute value and find index of minimum.
    
    Differentiate between 2-D and 1-D lat/lon arrays.
    for 2-D arrays, should receive values in this format: dd=[lat, lon], dd_array=[lats2d,lons2d]
  '''
  if type == "lon" and len(dd_array.shape) == 1:
    dd_array = np.where(dd_array <= 180, dd_array, dd_array - 360)

  if (len(dd_array.shape) < 2):
    geo_idx = (np.abs(dd_array - dd)).argmin()
  else:
    if (dd_array[1] < 0).any():
      dd_array[1] = np.where(dd_array[1] <= 180, dd_array[1], dd_array[1] - 360)

    a = abs( dd_array[0]-dd[0] ) + abs(  np.where(dd_array[1] <= 180, dd_array[1], dd_array[1] - 360) - dd[1] )
    i,j = np.unravel_index(a.argmin(), a.shape)
    geo_idx = [i,j]

  return geo_idx

if __name__ == '__main__':
  main()