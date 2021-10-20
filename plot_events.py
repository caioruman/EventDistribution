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
  width = 0.75

  for i, city in enumerate(cities):

    fig, ax = plt.subplots(figsize=(14,10))

    ax1 = ax.bar(labels, events[:,i,1], width, label='10-20')    

    ax2 = ax.bar(labels, events[:,i,2], width, bottom=events[:,i,1],
          label='20-30')
    ax3 = ax.bar(labels, events[:,i,3], width, bottom=events[:,i,1] + events[:,i,2],
          label='30-40')
    ax4 = ax.bar(labels, events[:,i,4], width, bottom=events[:,i,1] + events[:,i,2] + events[:,i,3],
          label='40-50')
    ax5 = ax.bar(labels, events[:,i,5], width, bottom=events[:,i,1] + events[:,i,2] + events[:,i,3] + events[:,i,4],
          label='50+')

    y_offset = -5
    for r1, r2, r3, r4, r5 in zip(ax1, ax2, ax3, ax4, ax5):
      h1 = r1.get_height()
      h2 = r2.get_height()
      h3 = r3.get_height()
      h4 = r4.get_height()
      h5 = r5.get_height()
      
      if h1 != 0:
        y_offset = -5
        if h1 < 4:
          y_offset = -2
        plt.text(r1.get_x() + r1.get_width() / 2., h1 + r1.get_y() + y_offset, "%d" % h1, ha="center", va="bottom", color="k", fontsize=10, fontweight="bold")
      if h2 != 0:       
        y_offset = -5
        if h2 < 4:
          y_offset = -2
        plt.text(r2.get_x() + r2.get_width() / 2., h2 + r2.get_y()+ y_offset, "%d" % h2, ha="center", va="bottom", color="k", fontsize=10, fontweight="bold")
      if h3 != 0:
        y_offset = -5
        if h3 < 5:
          y_offset = -2
        plt.text(r3.get_x() + r3.get_width() / 2., h3 + r3.get_y()+ y_offset, "%d" % h3, ha="center", va="bottom", color="k", fontsize=10, fontweight="bold")
      if h4 != 0:
        y_offset = -5
        if h4 < 4:
          y_offset = -2
        plt.text(r4.get_x() + r4.get_width() / 2., h4 + r4.get_y() + y_offset, "%d" % h4, ha="center", va="bottom", color="k", fontsize=10, fontweight="bold")
      if h5 != 0:
        y_offset = -5
        if h5 < 4:
          y_offset = -2
        plt.text(r5.get_x() + r5.get_width() / 2., h5 + r5.get_y() + y_offset, "%d" % h5, ha="center", va="bottom", color="k", fontsize=10, fontweight="bold")

      

    ax.set_ylim(0,125)
    ax.set_ylabel('Number of events', fontsize=22)
    plt.xticks(fontsize=22)
    plt.yticks(fontsize=22)
    ax.set_title(f'Monthly distribution of {var} for {city} in SWE (mm) - 2000-2013', fontsize=22)
    plt.grid(axis='y', linestyle = '--', linewidth = 0.5, color='gray')
    ax.legend(fontsize=20, loc="upper center")

    plt.savefig(f"{city}_{var}.png".replace(' ', '_'))

if __name__ == '__main__':
  main()