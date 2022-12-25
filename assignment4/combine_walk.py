import os
import pathlib
import sys
import numpy as np
import pandas as pd
from datetime import datetime
from xml.etree import ElementTree as ET


def output_gpx(points, output_filename):
    """
    Output a GPX file with latitude and longitude from the points DataFrame.
    """
    from xml.dom.minidom import getDOMImplementation, parse
    xmlns = 'http://www.topografix.com/GPX/1/0'
    
    def append_trkpt(pt, trkseg, doc):
        trkpt = doc.createElement('trkpt')
        trkpt.setAttribute('lat', '%.10f' % (pt['lat']))
        trkpt.setAttribute('lon', '%.10f' % (pt['lon']))
        time = doc.createElement('time')
        time.appendChild(doc.createTextNode(pt['timestamp'].strftime("%Y-%m-%dT%H:%M:%SZ")))
        trkpt.appendChild(time)
        trkseg.appendChild(trkpt)

    doc = getDOMImplementation().createDocument(None, 'gpx', None)
    trk = doc.createElement('trk')
    doc.documentElement.appendChild(trk)
    trkseg = doc.createElement('trkseg')
    trk.appendChild(trkseg)

    points.apply(append_trkpt, axis=1, trkseg=trkseg, doc=doc)

    doc.documentElement.setAttribute('xmlns', xmlns)

    with open(output_filename, 'w') as fh:
        fh.write(doc.toprettyxml(indent='  '))


def get_data(input_gpx):
        #Parse data
    tree = ET.parse(input_gpx)
    
    data = pd.DataFrame(columns=['lat', 'lon', 'timestamp'])
    
    #Append position and time from gpx file 
    for position,time in zip(tree.iter('{http://www.topografix.com/GPX/1/0}trkpt'),tree.iter('{http://www.topografix.com/GPX/1/0}time')):

        dataEntry = {'lat': [position.attrib['lat']], 'lon': [position.attrib['lon']], 'timestamp': [time.text]}
        entry = pd.DataFrame(dataEntry)
        data = data.append(entry) 
        
    #Convert from object dtype to numeric and datetime
    data['lat'] = pd.to_numeric(data['lat'])
    data['lon'] = pd.to_numeric(data['lon'])
    data['timestamp'] = pd.to_datetime(data['timestamp'], utc=True)
    
    return data


def process_data(accl_df,gps_df,phone_df):
    
    offsetList = np.linspace(-5,5,100)
    crossCorrVals = []
    
    first_time = accl_df['timestamp'].min()
    
    for offset in offsetList:

        accl = accl_df
        gps = gps_df
        phone = phone_df

        phone['timestamp'] = first_time + pd.to_timedelta(phone['time'], unit='sec') + pd.to_timedelta(offset, unit='s')


        #Rounding to the nearest 4 seconds
        phone['timestamp'] = phone['timestamp'].dt.round("4S")
        gps['timestamp'] = gps['timestamp'].dt.round("4S")
        accl['timestamp'] = accl['timestamp'].dt.round("4S")

        # Group every 4 second bins and find the mean
        phone = phone.groupby(['timestamp']).mean()
        gps = gps.groupby(['timestamp']).mean()
        accl = accl.groupby(['timestamp']).mean()


        df = pd.merge(left = phone, right = gps, how="inner", left_on = phone.index, right_on = gps.index)
        df = df.rename(columns = {'key_0' : 'timestamp'}).set_index(['timestamp'])
        df = pd.merge(left = df, right = accl, how="inner", left_on = df.index, right_on = accl.index)
        df = df.rename(columns = {'key_0' : 'timestamp'}).set_index(['timestamp'])

        ccArr = df['gFx']*df['x']
        crossCorr = ccArr.sum()
        crossCorrVals.append(crossCorr)
    
    maxCorrTime = offsetList[np.where(crossCorrVals == np.max(crossCorrVals))[0][0]]

    
    return df,maxCorrTime;


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


def main():
    input_directory = pathlib.Path(sys.argv[1])
    output_directory = pathlib.Path(sys.argv[2])
    
    accl = pd.read_json(input_directory / 'accl.ndjson.gz', lines=True, convert_dates=['timestamp'])[['timestamp', 'x', 'y']]
    gps = get_data(input_directory / 'gopro.gpx')
    phone = pd.read_csv(input_directory / 'phone.csv.gz')[['time', 'gFx', 'Bx', 'By']]

    # TODO: create "combined" as described in the exercise
    df, tLag = process_data(accl,gps,phone)
    
    df = df.reset_index()
    
    print(f'Best time offset: {tLag:.1f}')
    os.makedirs(output_directory, exist_ok=True)
    output_gpx(df[['timestamp', 'lat', 'lon']], output_directory / 'walk.gpx')
    df[['timestamp', 'Bx', 'By']].to_csv(output_directory / 'walk.csv', index=False)


main()
