# coding: utf-8

import numpy as np
import sys
import pickle
import matplotlib.pyplot as plt            # Module to produce figureimport matplotlib.colors as colors
#import cartopy.crs as ccrs                 # Import cartopy ccrs
#import cartopy.feature as cfeature         # Import cartopy common features
import matplotlib.colors


def main():

#  sim = "CTRL"
#  st = f"/chinook/marinier/CONUS_2D/{sim}"

  datai = 2000
  dataf = 2013  

  #store = '/chinook/cruman/Data/WetSnow' 

  city = ["Edmundston", "Bathurst", "Miramichi", "Moncton", "Fredericton", "Saint John"] 
  city_lat = [47.42, 47.63, 47.01, 46.11, 45.87, 45.32]
  city_lon = [-68.32, -65.74, -65.47, -64.68, -66.53, -65.89]

  # Month, city, events
  #events_wsn = np.zeros([12,6,6])
  #events_sn = np.zeros([12,6,6])
  #events_pr = np.zeros([12,6,6])

  '''
 Start parameters: 
 City, initial date, final date 
 Get the location of the city. Use it and the grids around it in the calculations. 
 For each month, get the continuous events of snow, sum it. Add it to the bin: [10, 20, 30, 40, 50, 50+] 
 Make the plot at the end, similar to this one:  
  '''

  # annual data
  i = 0
  for y in range(datai, dataf+1):
    print(f"Year: {y}")

#print(events_wsn)
#print(events_sn)
#print(events_pr)
#sys.exit()      
    i += 1
    #if i == 1:
    #    events_wsn = pickle.load( open( f"pickle/wet_snow_{y}_duration.p", "rb" ) )
    #    events_sn = pickle.load( open( f"pickle/snow_{y}_duration.p", "rb" ) )
    #    events_pr = pickle.load( open( f"pickle/rain_{y}_duration.p", "rb" ) )

    #else:
    aux_wsn = pickle.load( open( f"pickle/wet_snow_{y}_duration.p", "rb" ) )
    aux_sn = pickle.load( open( f"pickle/snow_{y}_duration.p", "rb" ) )
    aux_pr = pickle.load( open( f"pickle/rain_{y}_duration.p", "rb" ) )
    
    #print(aux_wsn.shape)
    #print(aux_sn.shape)
    #print(aux_pr.shape)
    if aux_sn.shape[2] == 240:                
      events_sn[:,:,:240] += aux_sn        
    else:
      events_sn += aux_sn
        
    events_wsn += aux_wsn
    events_pr += aux_pr
      
  sn_sum = np.sum(events_sn, axis=0)
  wsn_sum = np.sum(events_wsn, axis=0)
  pr_sum = np.sum(events_pr[:,:,:320] - events_sn, axis=0)    

  sn_6bins = np.zeros([6,54])
  wsn_6bins = np.zeros([6,54])
  pr_6bins = np.zeros([6,54])

  # Sum of the events and dividing by 6hours bins
  aux = [np.sum(sn_sum[:,x:x+6],axis=1) for x in np.arange(0,sn_sum.shape[1],6)]

  for i, item in enumerate(aux):
    sn_6bins[:,i] = item

  aux = [np.sum(wsn_sum[:,x:x+6],axis=1) for x in np.arange(0,wsn_sum.shape[1],6)]
  for i, item in enumerate(aux):
    wsn_6bins[:,i] = item

  aux = [np.sum(pr_sum[:,x:x+6],axis=1) for x in np.arange(0,pr_sum.shape[1],6)]
  for i, item in enumerate(aux):
    pr_6bins[:,i] = item
  
  # 0-10, 10-20, 20-30, 30-40, 40-50, 50+
  labels = np.arange(0,320,6)
  
  for c in enumerate(city):
    plotEventDuration(wsn_6bins[c,:], labels, city[c], 'Wet Snow', 'Wet Snow')
    plotEventDuration(sn_6bins[c,:], labels, city[c], 'Snow', 'Snow')
    plotEventDuration(pr_6bins[c,:], labels, city[c], 'Liquid Precipitation', 'Liquid Precipitation')    
      
def plotEventDuration(bins, labels, city, xlabel, fname):
  fig, ax = plt.subplots(figsize=(16,10))
  width=4
  #labels = np.arange(0,320,6)
  ax1 = ax.bar(labels[1:20], bins[0,1:20], width)
  plt.xticks(np.arange(0,320,6)[1:20],fontsize=22)
  plt.yticks(np.arange(0,1401,200), fontsize=22)
  plt.xlabel(f'{xlabel} duration [hours]', fontsize=22)
  plt.grid(axis='y', linestyle = '--', linewidth = 0.5, color='gray')
  plt.title(f'{city}, NB', fontsize=26)
  
  plt.savefig(f'{fname}_{city}_duration.png'.replace(' ', '_'))



if __name__ == '__main__':
  main()