import sys
import pandas as pd
import numpy as np
from pykalman import KalmanFilter
from xml.etree import ElementTree as ET


def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation
    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.7f' % (pt['lat']))
        trkpt.setAttribute('lon', '%.7f' % (pt['lon']))
        trkseg.appendChild(trkpt)
    
    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)
    
    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)
    
    with open(output_filename, 'w') as fh:
        doc.writexml(fh, indent=' ')


def input_gpx(name_gpx,name_csv):
    
    #Parse data
    tree = ET.parse(name_gpx)
    
    data = pd.DataFrame(columns=['lat', 'lon', 'datetime'])
    
    #Append position and time from gpx file 
    for position,time in zip(tree.iter('{http://www.topografix.com/GPX/1/0}trkpt'),tree.iter('{http://www.topografix.com/GPX/1/0}time')):

        dataEntry = {'lat': [position.attrib['lat']], 'lon': [position.attrib['lon']], 'datetime': [time.text]}
        entry = pd.DataFrame(dataEntry)
        data = data.append(entry) 
        
    #Convert from object dtype to numeric and datetime
    data['lat'] = pd.to_numeric(data['lat'])
    data['lon'] = pd.to_numeric(data['lon'])
    data['datetime'] = pd.to_datetime(data['datetime'], utc=True)

    
    #Read csv file
    data2 = pd.read_csv(name_csv, parse_dates=['datetime'])

    #Merge XML and CSV File
    data['Bx'] = data2['Bx']
    data['By'] = data2['By']
    
    data = data.set_index('datetime')
    
    return data
    

def haversine(lat1, lon1, lat2, lon2):
    '''
     # Adapted from EdChum's answer 
    https://stackoverflow.com/questions/25767596/vectorised-haversine-formula-with-a-pandas-dataframe
    '''
    
    lon1, lat1, lon2, lat2 = map(np.radians, [lon1, lat1, lon2, lat2])
    dlon = lon2 - lon1 
    dlat = lat2 - lat1 
    a = np.sin(dlat/2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon/2)**2
    c = 2 * np.arcsin(np.sqrt(a)) 
    km = 6367 * c
    
    return km*1000


def distance(inputData):
    
    data = inputData
    
    data['lat1'] = data['lat'].shift(-1).fillna(0)
    data['lon1'] = data['lon'].shift(-1).fillna(0)
    
    data['ds'] = data.apply(lambda x: haversine(x['lat'],x['lon'],x['lat1'],x['lon1']), axis=1)
      
    s = np.sum(data['ds'][:-1])
    
    return s


def smooth(inputData):
    
    data = inputData
    
    initial_state = data.iloc[0]
    observation_covariance = np.diag([0.5, 0.5, 0.8, 0.8]) ** 2 # TODO: shouldn't be zero
    transition_covariance = np.diag([0.4, 0.4, 0.8, 0.8]) ** 2 # TODO: shouldn't be zero
    transition = [[1,0,6*10**-7,29*10**-7], [0,1,-43*10**-7,12*10**-7], [0,0,1,0], [0,0,0,1]] # TODO: shouldn't (all) be zero
    
    kf = KalmanFilter(
    initial_state_mean = initial_state,
    initial_state_covariance = observation_covariance,
    observation_covariance = observation_covariance,
    transition_covariance = transition_covariance,
    transition_matrices = transition
    )
    
    #filtered data
    smoothedData = np.array(kf.smooth(data)[0]);

    smoothedData = pd.DataFrame(smoothedData, columns=['lat','lon','Bx','By'])
    
    #adding timestamps
    smoothedData['datetime'] = data.reset_index()['datetime']
    smoothedData.set_index('datetime')
    
    return smoothedData


def main():
    nameGPX = sys.argv[1]
    nameCSV = sys.argv[2]
    
    data = input_gpx(nameGPX,nameCSV)

    dist = distance(data)
    print(f'Unfiltered distance: {dist:.2f}')
    
    data = input_gpx(nameGPX,nameCSV)

    smoothed_points = smooth(data)
    smoothed_dist = distance(smoothed_points)
    print(f'Filtered distance: {smoothed_dist:.2f}')

    output_gpx(smoothed_points, 'out.gpx')


if __name__ == '__main__':
    main()
