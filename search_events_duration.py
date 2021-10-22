# coding: utf-8

import xarray as xr
import numpy as np
import pandas as pd
from glob import glob
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pickle
#import matplotlib.pyplot as plt            # Module to produce figureimport matplotlib.colors as colors
#import cartopy.crs as ccrs                 # Import cartopy ccrs
#import cartopy.feature as cfeature         # Import cartopy common features
#import matplotlib.colors


def main():

  sim = "CTRL"
  st = f"/chinook/marinier/CONUS_2D/{sim}"

  datai = 2000
  dataf = 2013  

  store = '/chinook/cruman/Data/WetSnow' 

  city = ["Edmundston", "Bathurst", "Miramichi", "Moncton", "Fredericton", "Saint John"] 
  city_lat = [47.42, 47.63, 47.01, 46.11, 45.87, 45.32]
  city_lon = [-68.32, -65.74, -65.47, -64.68, -66.53, -65.89]

  # Month, city, events
  events_wsn = np.zeros([12,6,6])
  dur_events_wsn = np.zeros([12,6,120])
  events_sn = np.zeros([12,6,6])
  dur_events_sn = np.zeros([12,6,120])
  events_pr = np.zeros([12,6,6])
  dur_events_pr = np.zeros([12,6,120])

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

      if (y == 2000) and m < 10:
        continue

      if (y == 2013) and m > 9:
        continue

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

      pr_lat = pr_lat[i1:i2,j1:j2]  
      pr_lon = pr_lon[i1:i2,j1:j2]  

      print(f"Month: {m}")

      datai = datetime.strptime(f'{y}-{m:02d}-01 00:00', '%Y-%m-%d %H:%M')
      dataf = datetime.strptime(f'{y}-{m:02d}-01 00:00', '%Y-%m-%d %H:%M')
      dataf += relativedelta(months=1)
      

      #print(wsn)

      wsn = wsn.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      sn = sn.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      uu = uu.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      vv = vv.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      pr = pr.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))  
      pr = pr - sn

      #print(wsn)    

      for k, coords in enumerate(zip(city_lat, city_lon)):

        lat = coords[0]
        lon = coords[1]

        i, j = geo_idx([lat, lon], np.array([wsn.XLAT, wsn.XLONG]))

        ii, jj = geo_idx([lat, lon], np.array([pr_lat, pr_lon]))

        aux_wsn = wsn.SN_4C[:,i,j]
        aux_sn = sn[:,ii,jj]
        aux_uu = uu[:,ii,jj]
        aux_vv = vv[:,ii,jj]
        aux_pr = pr[:,ii,jj]

        events_wsn[m-1,k,:], dur_events_wsn[m-1,k,:] = getEvents(aux_wsn, events_wsn[m-1,k,:], dur_events_wsn[m-1,k,:])    
        events_sn[m-1,k,:], dur_events_sn[m-1,k,:] = getEvents(aux_sn, events_sn[m-1,k,:], dur_events_sn[m-1,k,:])
        events_pr[m-1,k,:], dur_events_pr[m-1,k,:] = getEvents(aux_pr, events_pr[m-1,k,:], dur_events_pr[m-1,k,:])

        # 0-10, 10-20, 20-30, 30-40, 40-50, 50+
        events_limits = [10,20,30,40,50,60]
    
    #print(events_wsn)
    #print(events_sn)
    #print(events_pr)
    #sys.exit()      

    pickle.dump( events_wsn, open( f"wet_snow_{y}_v2.p", "wb" ) )
    pickle.dump( events_sn, open( f"snow_{y}_v2.p", "wb" ) )
    pickle.dump( events_pr, open( f"rain_{y}_v2.p", "wb" ) )    
    pickle.dump( dur_events_wsn, open( f"wet_snow_{y}_duration.p", "wb" ) )
    pickle.dump( dur_events_sn, open( f"snow_{y}_duration.p", "wb" ) )
    pickle.dump( dur_events_pr, open( f"rain_{y}_duration.p", "wb" ) ) 


def getEvents(data, events, dur_events):

  # 0-10, 10-20, 20-30, 30-40, 40-50, 50+
  aux = 0
  i = 0
  dur = 0

  for k in range(len(data)):
    item = data[k]

    # Check each item in the array. While not 0, add to aux. When 0, store aux, set it to 0. Get the max wind between the start and end.
    if np.isnan(item) and aux == 0:
      continue
    if item == 0 and aux == 0:
      continue
    elif item == 0 or np.isnan(item):
      # Streak ended
      # Waiting 6 hours before going to the next event
      i += 1
      if i < 6:
        continue
      else:      
        index = int(aux/10)
        if index >= 6:
          index = 5

      # get max wind
      # for later
      # store aux
        events[index] += 1
        dur_events[dur] += 1
      #print(i)
      # reset aux
        aux = 0
        dur = 0
        i = 0
    else: #if item != 0 and not np.isnan(item):
      aux += item
      i = 0
      dur += 1
      #i += 1

  return events, dur_events

             
          
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