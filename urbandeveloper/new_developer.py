__author__ = 'jmartinez'
import pandas as pd, numpy as np
import sys, time, random, string, os
from synthicity.utils import misc
import multiprocessing as mp
from urbandeveloper import elasticity_model_2SLS
from sqlalchemy import *

RESUNITSIZE = 1500
inv_type_d = {
  #'2,3,20,24,11': 'Residential',
  '2,3,20,24': 'Residential',
  '9,22': 'Industrial',
  '17,18': 'Retail',
  '5': 'Office'
}
for k,v in inv_type_d.items():
  for btyp in string.split(k,','): inv_type_d[int(btyp)]=v
  del inv_type_d[k]


def get_simulated_demand(zone_id,hh_zone_diff,emp_zone_diff,zone_args):
    if(zone_id in hh_zone_diff.index):
        hh_diff = hh_zone_diff.loc[zone_id]

        if zone_args is not None:
            subsidized_demand = zone_args.subsidized_hh_demand[zone_id]
            hh_diff = hh_diff + subsidized_demand
            hh_diff = hh_diff * 300
    else:
        hh_diff = 0

    if(zone_id in emp_zone_diff.index):
        emp_diff = emp_zone_diff.loc[zone_id]

        if zone_args is not None:
            subsidized_demand = zone_args.subsidized_emp_demand[zone_id]
            emp_diff = emp_diff * 50

    else:
        emp_diff = 0

    return [hh_diff, emp_diff]




# map function
def calculate_parcels(series, d):
    btype = int(series[0])
    parcel_id = int(series[1])
    parcel_prediction = series[2]
    zone_id = int(series[3])
    county_id = series[4]
    parcel_sqft = series[5]


    if(d[zone_id][2] == 0):

        if (int(btype) in [2,3,20,24]):
            hh_remaining_sqft = d[zone_id][0] - parcel_sqft
            if (hh_remaining_sqft > -500000):
                parcel_btype = (parcel_id, btype, parcel_sqft)
                d[zone_id].append(parcel_btype)
                d[zone_id][0] = hh_remaining_sqft
        else:
            emp_remaining_sqft = d[zone_id][1] - parcel_sqft
            if (emp_remaining_sqft > -500000):
                parcel_btype = (parcel_id, btype, parcel_sqft)
                d[zone_id].append(parcel_btype)
                d[zone_id][1] = emp_remaining_sqft

def run_job(df, d):
    res = df.apply(calculate_parcels, axis=1, args=(d,))
    return d

def run(dset,hh_zone_diff,emp_zone_diff,parcel_predictions,year=2010,min_building_sqft=400,min_lot_sqft=250,max_lot_sqft=500000,zone_args=None, tot_sqft=None):

    MINBUILDINGSQFT = min_building_sqft
    MAXLOTSIZE = max_lot_sqft
    MINLOTSIZE = min_lot_sqft
    #build structure for looping
    parcels=dset.parcels
    if parcels.index.name !='parcel_id':
        parcels = parcels.set_index(parcels['parcel_id'])
    parcel_predictions['zone_id'] = parcels.zone_id.ix[parcel_predictions.index]
    parcel_predictions['sqft'] = parcels.loc[parcel_predictions.index, 'parcel_sqft']


    pp = parcel_predictions.unstack().reset_index()
    pp.columns = ['btype', 'parcel_id', 'value']
    counties = pp.loc[pp.btype == 'county_id', ['parcel_id', 'value']]
    zones = pp.loc[pp.btype == 'zone_id', ['parcel_id', 'value']]
    sqft = pp.loc[pp.btype == 'sqft', ['parcel_id', 'value']]

    constraint = ['zone_id', 'county_id', 'sqft']
    pp = pp.loc[~pp.btype.isin(constraint), :]

    pp = pd.merge(pp, zones, on='parcel_id')
    pp = pd.merge(pp, counties, on='parcel_id')
    pp = pd.merge(pp, sqft, on='parcel_id')

    pp.columns = ['btype', 'parcel_id', 'parcel_prediction', 'zone_id', 'county_id', 'parcel_sqft']
    pp = pp.dropna()
    pp = pp.sort(['parcel_prediction'], ascending=[0])



    # create result structures


    d = {}
    zone_index = dset.zones.index.values
    for i in zone_index:
        result_l = []
        demand_sqft = get_simulated_demand(i, hh_zone_diff, emp_zone_diff, zone_args)
        result_l.append(demand_sqft[0])
        result_l.append(demand_sqft[1])
        result_l.append(int(zone_args.loc[i, "no_build"]))
        d[i] = result_l




    #map rows to result data structure

    from functools import partial
    mapfunc = partial(run_job, d=d)

    p = mp.Pool(processes=4)
    split_dfs = np.array_split(pp, 4)
    pool_results = p.map(mapfunc, split_dfs)
    p.close()
    p.join()


    results = pd.DataFrame.from_dict(pool_results[0], orient='index')

    #build price shifter data structure

    price_shifters = pd.DataFrame(index=results.index, columns=['2','3','20','24','5', '17', '18', '9', '22', 'res_sqft', 'non_res_sqft'])
    price_shifters['res_sqft'] = tot_sqft['residential_sqft_zone']
    price_shifters['non_res_sqft'] = tot_sqft['non_residential_sqft_zone']
    price_shifters['2'] = results.loc[:, 0] / price_shifters['res_sqft']
    price_shifters['3'] = results.loc[:, 0] / price_shifters['res_sqft']
    price_shifters['20'] =  results.loc[:, 0] / price_shifters['res_sqft']
    price_shifters['24'] =  results.loc[:, 0] / price_shifters['res_sqft']
    price_shifters['5'] =  results.loc[:, 1] / price_shifters['non_res_sqft']
    price_shifters['17'] =  results.loc[:, 1] / price_shifters['non_res_sqft']
    price_shifters['18'] =  results.loc[:, 1] / price_shifters['non_res_sqft']
    price_shifters['9'] =  results.loc[:, 1] / price_shifters['non_res_sqft']
    price_shifters['22'] =  results.loc[:, 1] / price_shifters['non_res_sqft']

    price_shifters = price_shifters.ix[:, 0:8]
    price_shifters = price_shifters.stack()
    price_shifters = price_shifters.loc[price_shifters>0]
    #this is where I would run the 2SLS model and adjust prices
    price_shifters_d = price_shifters.to_dict()

    results = results.loc[:, 3:len(results.columns)-1].transpose()
    results = results.loc[:, ~results.iloc[0].isnull()]
    results = results.stack()
    results = results.reset_index()
    newbuildings = results.loc[:, 0].apply(pd.Series)
    newbuildings.columns = ['parcel_id', 'building_type_id', 'building_sqft']




    #report stats on buildings
    print newbuildings.groupby('building_type_id').size()
    print "Total buildings built: ", newbuildings.groupby('building_type_id').size().sum()

    newbuildings['general_type'] = newbuildings['building_type_id'].map(inv_type_d)
    newbuildings = pd.merge(newbuildings, dset.parcels, on='parcel_id')[['parcel_id', 'building_type_id', 'building_sqft', 'general_type', 'parcel_sqft']]
    newbuildings["zone_id"] = results.loc[:, "level_1"]
    newbuildings = newbuildings.drop_duplicates(subset='parcel_id')
    newbuildings = newbuildings.set_index('parcel_id')
    newbuildings = newbuildings.rename(columns={"parcel_sqft": "lot_size"})
    newbuildings['residential_units'] = np.ceil((newbuildings.general_type=='Residential')*newbuildings.building_sqft/RESUNITSIZE)
    newbuildings = newbuildings[newbuildings.lot_size<MAXLOTSIZE]
    newbuildings = newbuildings[newbuildings.lot_size>MINLOTSIZE]

    return newbuildings, price_shifters_d




    #price_shifters.to_csv('c:/users/jmartinez/documents/test_results.csv')

sqft = pd.DataFrame()
if __name__ == '__main__':


  import dataset
  import cProfile


  dset = dataset.DRCOGDataset(os.path.join(misc.data_dir(),'drcog.h5'))


  #add variables for test sim
  emp_zone_diff = pd.read_csv('C:/Users/jmartinez/Documents/Projects/UrbanSim/Developer/emp_zone_diff.csv', index_col=0)
  hh_zone_diff = pd.read_csv('C:/Users/jmartinez/Documents/Projects/UrbanSim/Developer/hh_zone_diff.csv', index_col=0)
  parcel_predictions = pd.read_csv('C:/Users/jmartinez/Documents/Projects/UrbanSim/Developer/parcel_predictions2.csv', index_col=0)
  zone_args = pd.read_csv('C:/Users/jmartinez/Documents/Projects/UrbanSim/Developer/zone_args.csv', index_col=0)
  tot_sqft = pd.read_csv('C:/Users/jmartinez/Documents/Projects/UrbanSim/Developer/tot_sqft.csv', index_col=0)
  min_building_sqft = 400
  min_lot_sqft = 500
  max_parcel_sqft = 200000

  fnc = "newbuildings, price_shifters = run(dset,hh_zone_diff,emp_zone_diff,parcel_predictions,year=2011," +\
                             "min_building_sqft=min_building_sqft," +\
                             "min_lot_sqft=min_lot_sqft," +\
                             "max_lot_sqft=max_parcel_sqft,zone_args=zone_args, tot_sqft=tot_sqft)"
  cProfile.run(fnc, "c:/users/jmartinez/documents/test_developer")