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
  dataf = 2004  

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
    if i == 1:
      events_wsn = pickle.load( open( f"wet_snow_{y}.p", "rb" ) )
      events_sn = pickle.load( open( f"snow_{y}.p", "rb" ) )
      events_pr = pickle.load( open( f"rain_{y}.p", "rb" ) )
      
    else:
      aux_wsn = pickle.load( open( f"wet_snow_{y}.p", "rb" ) )
      aux_sn = pickle.load( open( f"snow_{y}.p", "rb" ) )
      aux_pr = pickle.load( open( f"rain_{y}.p", "rb" ) )
      
      events_wsn += aux_wsn
      events_sn += aux_sn
      events_pr += aux_pr
      
      
  
  # 0-10, 10-20, 20-30, 30-40, 40-50, 50+
  events_limits = [10,20,30,40,50,60]
  plotBars(events_wsn, city, 'Wet Snow')
  plotBars(events_sn, city, 'Snow')
  plotBars(events_pr, city, 'Liquid Precipitation')
      

def plotBars(events, cities, var):

  labels = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
  width = 0.35

  for i, city in enumerate(cities):

    fig, ax = plt.subplots()

    ax.bar(labels, events[:,i,1], width, label='10-20')
    ax.bar(labels, events[:,i,2], width, bottom=events[:,i,1],
          label='20-30')
    ax.bar(labels, events[:,i,3], width, bottom=events[:,i,2],
          label='30-40')
    ax.bar(labels, events[:,i,4], width, bottom=events[:,i,3],
          label='40-50')
    ax.bar(labels, events[:,i,5], width, bottom=events[:,i,4],
          label='50+')

    ax.set_ylabel('SWE (mm)')
    ax.set_title(f'Monthly distribution of {var} for {city} - 2000-2013')
    ax.legend()

    plt.savefig(f"{city}_{var}.png")

if __name__ == '__main__':
  main()