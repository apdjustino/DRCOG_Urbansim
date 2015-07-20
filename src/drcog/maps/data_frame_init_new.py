__author__ = 'jmartinez'
import numpy as np, pandas as pd, os
from drcog.maps import dframe_explorer_new



# z2015 = pd.read_csv('//kennedy/CRS/Urban Sim/UrbanSim Final RTP data folder/data/drcog2/runs/zone_summary2015_091714071811.csv')
# z2020 = pd.read_csv('//kennedy/CRS/Urban Sim/UrbanSim Final RTP data folder/data/drcog2/runs/zone_summary2020_091714074157.csv')
# z2025 = pd.read_csv('//kennedy/CRS/Urban Sim/UrbanSim Final RTP data folder/data/drcog2/runs/zone_summary2025_091714082039.csv')
# z2030 = pd.read_csv('//kennedy/CRS/Urban Sim/UrbanSim Final RTP data folder/data/drcog2/runs/zone_summary2030_091714095214.csv')
# z2035 = pd.read_csv('//kennedy/CRS/Urban Sim/UrbanSim Final RTP data folder/data/drcog2/runs/zone_summary2035_091714111246.csv')
# z2040 = pd.read_csv('//kennedy/CRS/Urban Sim/UrbanSim Final RTP data folder/data/drcog2/runs/zone_summary2040_091714132335.csv')

data = pd.read_csv('C:/users/jmartinez/documents/projects/urbansim/results/zone_summary.csv')
d = {'zone_data':data}


#d = {'2015': z2015, '2020':z2020, '2025':z2025, '2030':z2030, '2035':z2030, '2040':z2040}

dframe_explorer_new.start(d,
        center=[39.7722557, -105.0487455],
        zoom=11,
        shape_json='/data/zones.geojson',
        geom_name='ZONE_ID', # from JSON file
        join_name='zone_id', # from data frames
        precision=2)