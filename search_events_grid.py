# coding: utf-8

import xarray as xr
import numpy as np
import pandas as pd
from glob import glob
import sys
from datetime import datetime
from dateutil.relativedelta import relativedelta
import pickle
import dask
from dask import delayed
#import matplotlib.pyplot as plt            # Module to produce figureimport matplotlib.colors as colors
#import cartopy.crs as ccrs                 # Import cartopy ccrs
#import cartopy.feature as cfeature         # Import cartopy common features
#import matplotlib.colors

def main():

  from dask.distributed import Client, progress
  client = Client(threads_per_worker=10, n_workers=5)
  client

  sim = "CTRL"
  st = f"/chinook/marinier/CONUS_2D/{sim}"

  datai = 2000
  dataf = 2000  

  store = '/chinook/cruman/Data/WetSnow' 

  ns = 1e-9

  #city = ["Edmundston", "Bathurst", "Miramichi", "Moncton", "Fredericton", "Saint John"] 
  #city_lat = [47.42, 47.63, 47.01, 46.11, 45.87, 45.32]
  #city_lon = [-68.32, -65.74, -65.47, -64.68, -66.53, -65.89]

  # Month, city, events
  #events_wsn = np.zeros([12,6,6])
  #dur_events_wsn = np.zeros([12,6,240])
  #events_sn = np.zeros([12,6,6])
  #dur_events_sn = np.zeros([12,6,320])
  #events_pr = np.zeros([12,6,6])
  #dur_events_pr = np.zeros([12,6,480])

  #events_wsn = np.empty((12,153,166), dtype=object)
  #events_sn = np.empty((12,153,166), dtype=object)

  events_wsn = np.empty((153,166), dtype=object)
  events_sn = np.empty((153,166), dtype=object)

  #for i in range(events_sn.shape[0]):
  #  for j in range(events_sn.shape[1]):
  #    for k in range(events_sn.shape[2]):
  #      events_sn[i,j,k] = []
  #      events_wsn[i,j,k] = []

  for i in range(events_sn.shape[0]):
    for j in range(events_sn.shape[1]):
      #for k in range(events_sn.shape[2]):
      events_sn[i,j] = []
      events_wsn[i,j] = []

  '''
 Start parameters: 
 City, initial date, final date 
 Get the location of the city. Use it and the grids around it in the calculations. 
 For each month, get the continuous events of snow, sum it. Add it to the bin: [10, 20, 30, 40, 50, 50+] 
 Make the plot at the end, similar to this one:  
  '''
  aux = 0
  t = 0
  dur = 0
  aux_sn = 0
  t_sn = 0
  dur_sn = 0
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
      #uu = xr.open_dataset(f'{st}/{y}/wrf2d_d01_CTRL_EU10_{y}{mi:02d}-{y}{mf:02d}.nc', engine='netcdf4')
      #vv = xr.open_dataset(f'{st}/{y}/wrf2d_d01_CTRL_EV10_{y}{mi:02d}-{y}{mf:02d}.nc', engine='netcdf4')
      #pr = xr.open_dataset(f'{st}/{y}/wrf2d_d01_CTRL_PREC_ACC_NC_{y}{mi:02d}-{y}{mf:02d}.nc', engine='netcdf4')

      # Slicing the domain to make the computations faster
      i1=721; j1=1167; i2=874; j2=1333
      
      sn = sn.SNOW_ACC_NC
      wsn = wsn.SN_4C
      xlat = sn.XLAT
      xlon = sn.XLONG
      sn = sn[:,i1:i2,j1:j2] 
        
      #uu = uu.EU10
      #uu = uu[:,i1:i2,j1:j2]
        
      #vv = vv.EV10
      #vv = vv[:,i1:i2,j1:j2]
  
      #pr = pr.PREC_ACC_NC
      #pr = pr[:,i1:i2,j1:j2]  

      xlat = xlat[i1:i2,j1:j2]  
      xlon = xlon[i1:i2,j1:j2]  

      print(f"Month: {m}")

      datai = datetime.strptime(f'{y}-{m:02d}-01 00:00', '%Y-%m-%d %H:%M')
      dataf = datetime.strptime(f'{y}-{m:02d}-01 00:00', '%Y-%m-%d %H:%M')
      dataf += relativedelta(months=1)
    
      #print(wsn)

      wsn = wsn.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      sn = sn.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      #uu = uu.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      #vv = vv.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      #pr = pr.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))  
      #pr = pr - sn      

      for i in range(wsn.shape[1]):
        for j in range(wsn.shape[2]):
          #print('2')
          datei = datetime.utcfromtimestamp(wsn[0].Time.values.astype(int)*ns)
          #print('3')
          events_wsn[i,j] = getEvents(wsn[:,i,j], events_wsn[i,j], datei)
          events_sn[i,j] = getEvents(sn[:,i,j], events_sn[i,j], datei)
          #sys.exit()    
      #events_wsn = computeArray(wsn, events_wsn, m)
      #events_sn = computeArray(sn, events_sn, m)
        #events_pr[m-1,k,:], dur_events_pr[m-1,k,:] = getEvents(aux_pr, events_pr[m-1,k,:], dur_events_pr[m-1,k,:])
      #print(events_sn[m,:,:])
      
      #events_wsn = events_wsn.compute()
      #events_sn = events_sn.compute()
        # 0-10, 10-20, 20-30, 30-40, 40-50, 50+
      #  events_limits = [10,20,30,40,50,60]
    
    #print(events_wsn)
    #print(events_sn)
    #print(events_pr)
    #sys.exit()      
      print('start computing dask stuff')
      shape = events_wsn.shape
      results_wsn = dask.compute(*events_wsn.flatten())
      results_sn = dask.compute(*events_sn.flatten())

      results_wsn_p = np.array(results_wsn, dtype=object).reshape(shape)
      results_sn_p = np.array(results_sn, dtype=object).reshape(shape)
      print('writing pickles')
      pickle.dump( results_wsn_p, open( f"wet_snow_{y}_{m:02d}_v3.p", "wb" ) )
      pickle.dump( results_sn_p, open( f"snow_{y}__{m:02d}_v3.p", "wb" ) )

      events_wsn = np.empty((153,166), dtype=object)
      events_sn = np.empty((153,166), dtype=object)

      for i in range(events_sn.shape[0]):
        for j in range(events_sn.shape[1]):
          #for k in range(events_sn.shape[2]):
          events_sn[i,j] = []
          events_wsn[i,j] = []
    #pickle.dump( events_pr, open( f"rain_{y}_v2.p", "wb" ) )    
    #pickle.dump( dur_events_wsn, open( f"wet_snow_{y}_duration.p", "wb" ) )
    #pickle.dump( dur_events_sn, open( f"snow_{y}_duration.p", "wb" ) )
    #pickle.dump( dur_events_pr, open( f"rain_{y}_duration.p", "wb" ) ) 

#@delayed
def computeArray(wsn, events_wsn, m):
  ns = 1e-9
  for i in range(wsn.shape[1]):
    for j in range(wsn.shape[2]):
      #print('2')
      datei = datetime.utcfromtimestamp(wsn[0].Time.values.astype(int)*ns)
      #print('3')
      events_wsn[m-1,i,j] = getEvents(wsn[:,i,j], events_wsn[m-1,i,j], datei)
      #events_sn[m-1,i,j] = getEvents(sn[:,i,j], events_sn[m-1,i,j], datei)
      #sys.exit()
  return events_wsn#, events_sn

class Event:
  def __init__(self, length, intensity, initialDate): #, total):
    self.length = length
    self.intensity = intensity
    self.initialDate = initialDate
    #self.total = total
  def set_length(self, new_l):
    self.length = new_l
  def set_intensity(self, new_i):
    self.intensity = new_i
  def set_date(self, new_d):
    self.initialDate = new_d
  #def set_total(self, new_total):
    #self.total = new_total 

@delayed
def getEvents(data, events, dateIni, aux=0, i=0, dur=0):
  # This must also receive the values of aux, i and dur, so it can continue from the last month read

  # 0-10, 10-20, 20-30, 30-40, 40-50, 50+
  #aux = 0
  #i = 0
  #dur = 0
  
  # Each grid stores a list of the object Event, that contains the intensity, length, initial date, and total
  # total is initialized as the value from the previous event + 1
  # a = np.empty((3,3), dtype=object)
  # 1 Grid, the size of the domain.  

  for k in range(len(data)):
    item = data[k]

    # Check each item in the array. While not 0, add to aux. When 0, store aux, set it to 0. Get the max wind between the start and end.    
    if item == 0 and aux == 0:
      continue
    elif item == 0:
      # Streak ended
      # Waiting 6 hours before going to the next event
      # Making 3 hours to see how it changs
      i += 1
      if i < 6:
        dur += 1
        continue
      else:      
        #index = int(aux/10)
        #if index >= 6:
        #  index = 5

      # get max wind
      # for later
      # store aux
        if dur >= 23:
          date = dateIni + relativedelta(hours=k - i)
          events.append(Event(dur, aux, date))
        #events[index] += 1
        #dur_events[dur] += 1
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

  #if dur != 0:
  #  date = 
  return events, aux, i, dur

             

if __name__ == '__main__':
  main()