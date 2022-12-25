import numpy as np
import pandas as pd
import sys
from matplotlib import pyplot as plt


def read_data(stationName, cityName):
    cities = pd.read_csv(cityName)
    cities = cities[~cities['population'].isnull() & ~cities['area'].isnull()]
    cities['area'] = cities['area']/(1000**2)
    cities = cities[cities['area'] <= 10000] #filter
    cities['pop_density'] = cities['population']/cities['area']

    stations = pd.read_json(stationName, lines=True)
    stations['avg_tmax'] = stations['avg_tmax']/10
    
    return cities,stations


def distance(city, stations):
    '''
     # Adapted from EdChum's answer 
    https://stackoverflow.com/questions/25767596/vectorised-haversine-formula-with-a-pandas-dataframe
    '''    
    dlon = np.deg2rad(stations['longitude'] - city['longitude'])
    dlat = np.deg2rad(stations['latitude'] - city['latitude'])
    a = np.sin(dlat/2)**2 + np.cos(city['latitude']) * np.cos(stations['latitude']) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a)) 
    km = 6367 * c
    
    return km*1000


def best_tmax(city, stations):
    return stations.iloc[np.argmin(distance(city,stations))].avg_tmax


def main():
    
    stationsFile = sys.argv[1]
    citiesFile = sys.argv[2]
    outputFile = sys.argv[3]
    
    cities, stations = read_data(stationsFile, citiesFile)
    cities['tmax'] = cities.apply(best_tmax, stations = stations, axis = 1)
    
    plt.figure(figsize=(12,7))
    plt.scatter(cities['tmax'], cities['pop_density'], alpha=0.5, edgecolors='black')
    plt.xlabel("Avg Max Temperature (\u00b0C)", size = 'x-large')
    plt.ylabel("Population Density (people/km\u00b2)", size = 'x-large')
    plt.title("Plot of Population Density vs. Avg. Max Temperature", size = 'x-large');
    plt.savefig(outputFile)


main()
