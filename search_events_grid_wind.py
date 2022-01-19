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
import os

# First run for SN, than after that for WSN

def main():

  from dask.distributed import Client, LocalCluster, progress

  numberW = 5
  threadW = 4  

  sim = "CTRL"
  st = f"/chinook/marinier/CONUS_2D/{sim}"

  datai = 2011
  dataf = 2013

  store = '/chinook/cruman/Data/WetSnow' 

  ns = 1e-9


  events = np.empty((153,166), dtype=object)

  for i in range(events.shape[0]):
    for j in range(events.shape[1]):

      events[i,j] = []

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

#      if (m >= 5 and m <= 9):
#        continue

#      if (m < 5 or m > 9):
#        continue

      if os.path.exists(f"pr_acc_{y}_{m:02d}_v3.p"):
        print("jump")
        continue

      wsn = xr.open_dataset(f'{store}/{y}/WetSnow_SN_{y}{mi:02d}-{y}{mf:02d}.nc', engine='netcdf4')
      
      #/chinook/marinier/CONUS_2D/wrf2d_d01_CTRL_PREC_ACC_NC_200010-200012.nc      
      #sn = xr.open_dataset(f'{st}/{y}/wrf2d_d01_CTRL_SNOW_ACC_NC_{y}{mi:02d}-{y}{mf:02d}.nc', engine='netcdf4')
      uu = xr.open_dataset(f'{st}/{y}/wrf2d_d01_CTRL_EU10_{y}{mi:02d}-{y}{mf:02d}.nc', engine='netcdf4')
      vv = xr.open_dataset(f'{st}/{y}/wrf2d_d01_CTRL_EV10_{y}{mi:02d}-{y}{mf:02d}.nc', engine='netcdf4')
      pr = xr.open_dataset(f'{st}/{y}/wrf2d_d01_CTRL_PREC_ACC_NC_{y}{mi:02d}-{y}{mf:02d}.nc', engine='netcdf4')

      # Slicing the domain to make the computations faster
      i1=721; j1=1167; i2=874; j2=1333
      
      #sn = sn.SNOW_ACC_NC            
      wsn = wsn.SN_4C
      xlat = pr.XLAT
      xlon = pr.XLONG
      #sn = sn[:,i1:i2,j1:j2] 
        
      uu = uu.EU10
      uu = uu[:,i1:i2,j1:j2]
        
      vv = vv.EV10
      vv = vv[:,i1:i2,j1:j2]
  
      pr = pr.PREC_ACC_NC
      pr = pr[:,i1:i2,j1:j2]  

      xlat = xlat[i1:i2,j1:j2]  
      xlon = xlon[i1:i2,j1:j2]  

      print(f"Month: {m}")

      datai = datetime.strptime(f'{y}-{m:02d}-01 00:00', '%Y-%m-%d %H:%M')
      dataf = datetime.strptime(f'{y}-{m:02d}-01 00:00', '%Y-%m-%d %H:%M')
      dataf += relativedelta(months=1)
    
      #print(wsn)

      wsn = wsn.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      #sn = sn.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      uu = uu.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      vv = vv.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))
      pr = pr.sel(Time=slice(datai.strftime('%Y-%m-%d %H:%M'), dataf.strftime('%Y-%m-%d %H:%M')))  
      #pr = pr - sn  
      #          
      ws = np.sqrt(np.power(uu, 2) + np.power(vv, 2))

      for i in range(pr.shape[1]):
        for j in range(pr.shape[2]):
          #print('2')
          datei = datetime.utcfromtimestamp(pr[0].Time.values.astype(int)*ns)
          #print('3')          
          events[i,j] = getEvents(pr[:,i,j], wsn[:,i,j], ws[:,i,j], events[i,j], datei)
          #sys.exit()                      
            
      shape = events.shape      

      print('start computing dask stuff - SN')      

      with LocalCluster(n_workers=numberW,
        threads_per_worker=threadW,
        local_directory="/chinook/cruman/Scripts/EventDistribution/tmp",
      ) as cluster, Client(cluster) as client:
        # Do something using 'client'
        results_sn = dask.compute(*events.flatten())
        results_sn_p = np.array(results_sn, dtype=object).reshape(shape)

      print('writing pickles')
      pickle.dump( results_sn_p, open( f"pr_wsn_{y}_{m:02d}_v4.p", "wb" ) )
      
      events = np.empty((153,166), dtype=object)

      for i in range(events.shape[0]):
        for j in range(events.shape[1]):
          #for k in range(events_sn.shape[2]):
          events[i,j] = []          

class Event:
  def __init__(self, length, intensity, initialDate, wind, wind_count, wetSnow, wetSnow_count): #, total):
    self.length = length
    self.intensity = intensity
    self.initialDate = initialDate
    self.wind = wind    
    self.wetSnow = wetSnow
    self.wind_count = wind_count    
    self.wetSnow_count = wetSnow_count
    #self.total = total
  def set_length(self, new_l):
    self.length = new_l
  def set_intensity(self, new_i):
    self.intensity = new_i
  def set_date(self, new_d):
    self.initialDate = new_d
  # Array with the values
  # Array was taking too long. It's a 1 or 0 now
  def set_wind(self, new_w):
    self.wind = new_w  
  def set_wind_count(self, new_w_c):
    self.wind_count = new_w_c  
  # Array with the values
  # Array was taking too long. It's a 1 or 0 now
  def set_wetSnow(self, new_ws):
    self.wetSnow = new_ws
  def set_wetSnow_count(self, new_ws_c):
    self.wetSnow_count = new_ws_c
  #def set_total(self, new_total):
    #self.total = new_total 

@delayed
def getEvents(data, wsn, ws, events, dateIni, aux=0, i=0, dur=0, aux_ws=[], aux_wsn=[]):
  # This must also receive the values of aux, i and dur, so it can continue from the last month read    
  
  # Each grid stores a list of the object Event, that contains the intensity, length, initial date, wind for the duration of the event, wetSnow flag, and total
  # total is initialized as the value from the previous event + 1
  # a = np.empty((3,3), dtype=object)
  # 1 Grid, the size of the domain.    

  for k in range(len(data)):
    item = data[k].values
    
    # Check each item in the array. While not 0, add to aux. When 0, store aux, set it to 0. Get the max wind between the start and end.    
    if item == 0 and aux == 0:
      continue
    elif item == 0:
      # Streak ended
      # Waiting 6 hours before going to the next event      
      i += 1
      if i < 6:
        dur += 1
        aux_ws.append(ws[k].values)        
        aux_wsn.append(wsn[k].values)
        continue
      else:            
      # I'm saving all events greater than 12h
      # Later I can filter for the larger events
        if dur >= 12:
          date = dateIni + relativedelta(hours=k - i)
          # get uu, vv, wsn
          # code here
          wind = 0
          wind_count = 0
          if (np.max(aux_ws) >= 7.5):
            wind = 1
            wind_count = np.count_nonzero(np.greater(aux_ws, 7.5))
          wind_avg = np.mean(aux_ws)
          wind_max = np.max(aux_ws)
          wetSnow = 0
          wetSnow_count = 0
          wetSnow_total = 0
          if True in np.greater(aux_wsn, 0.1):
            wetSnow = 1
            wetSnow_count = np.count_nonzero(np.greater(aux_wsn, 0.1))
            wetSnow_total = np.sum(aux_wsn)
          events.append(Event(dur, aux, date, wind, wind_count, wind_avg, wind_max, wetSnow, wetSnow_count, wetSnow_total))
      # reset aux
        aux = 0
        dur = 0
        i = 0
        aux_ws = []        
        aux_wsn = []
    else:
      # ongoing event. Store data
      aux += item
      i = 0
      dur += 1
      aux_ws.append(ws[k].values)      
      aux_wsn.append(wsn[k].values)
      #i += 1
  
  return events#, aux, i, dur

             

if __name__ == '__main__':
  main()
