import pandas as pd, numpy as np
import sys, time, random, string, os
from synthicity.utils import misc
import spotproforma
import baydataset

# TODO better way of doing this?
def get_possible_rents_by_use(dset):
  parcels = dset.parcels 
  # need a prevailing rent for each parcel
  nodeavgrents = pd.read_csv('avenodeprice.csv',index_col='node_id')
  # convert from price per sqft to yearly rent per sqft
  nodeavgrents['rent'] = np.pmt(spotproforma.INTERESTRATE,spotproforma.PERIODS,nodeavgrents.price*-1)

  # need predictions of rents for each parcel
  avgrents = pd.DataFrame(index=parcels.index)
  avgrents['residential'] = nodeavgrents.rent.ix[parcels._node_id].values
  # make a stupid assumption converting the residential 
  # rent which I have to non-residential rent which I don't have
  avgrents['office'] = nodeavgrents.rent.ix[parcels._node_id].values*1.0
  avgrents['retail'] = nodeavgrents.rent.ix[parcels._node_id].values*.8
  avgrents['industrial'] = nodeavgrents.rent.ix[parcels._node_id].values*.5
  return avgrents

# TODO
# BIG QUESTION - should the above "possible rents" be greater than the
# here computed actual rents?  probably, right? 
def current_rent_per_parcel(far_predictions,avgrents):
  # this is bad - need the right rents for each type
  return far_predictions.total_sqft*avgrents.residential

def run(dset,year=2010,dev=None):

  parcels = dset.fetch('parcels').join(dset.fetch('zoning_for_parcels'),how="left")

  avgrents = get_possible_rents_by_use(dset)

  # compute total_sqft on the parcel, total current rent, and current far
  far_predictions = pd.DataFrame(index=parcels.index)
  far_predictions['total_sqft'] = dset.buildings.groupby('parcel_id').building_sqft.sum()
  far_predictions['total_sqft'] = far_predictions.total_sqft.fillna(0)
  far_predictions['currentrent'] = current_rent_per_parcel(far_predictions,avgrents)
  far_predictions['parcelsize'] = parcels.shape_area*10.764
  far_predictions.parcelsize[far_predictions.parcelsize<300] = 300 # some parcels have unrealisticly small sizes

  # do the lookup in the developer model - this is where the profitability is computed
  for form in spotproforma.forms.keys():
    far_predictions[form+'_feasiblefar'], far_predictions[form+'_profit'] = \
            dev.lookup(form,avgrents[spotproforma.uses].as_matrix(),far_predictions.currentrent,far_predictions.parcelsize)
  # we now have a far prediction per parcel

  print "Minimize feasibility vs. zoning:", time.ctime()
  zoning = dset.fetch('zoning').dropna(subset=['max_far'])

  parcels = pd.merge(parcels,zoning,left_on='zoning',right_index=True) # only keeps those with zoning

  # need to map building types in zoning to allowable forms in the developer model 
  type_d = { 
  'residential': [1,2,3],
  'industrial': [7,8,9],
  'retail': [10,11],
  'office': [4],
  'mixedresidential': [12],
  'mixedoffice': [14],
  }

  # we have zoning by like 16 building types and rents/far predictions by 4 building types
  # so we have to convert one into the other - would probably be better to have rents
  # segmented by the same 16 building types if we had good observations for that
  parcel_predictions = pd.DataFrame(index=parcels.index)
  for typ, btypes in type_d.iteritems():
    print typ, btypes
    for btype in btypes:

      # three questions - 1) is type allowed 2) what FAR is allowed 3) is it supported by rents
      tmp = parcels[parcels['type%d'%btype]=='t'][['max_far']] # is type allowed
      far_predictions['type%d_zonedfar'%btype] = tmp['max_far'] # at what far

      # merge zoning with feasibility
      tmp = pd.merge(tmp,far_predictions[[typ+'_feasiblefar']],left_index=True,right_index=True,how='left').set_index(tmp.index)

      # min of zoning and feasibility
      parcel_predictions[btype] = pd.Series(np.minimum(tmp['max_far'],tmp[typ+'_feasiblefar']),index=tmp.index) 
  parcel_predictions = parcel_predictions.dropna(how='all').sort_index(axis=1)

  print "Average rents\n", avgrents.describe()
  print "Feasibility\n", far_predictions.describe()
  print "Restricted to zoning\n", parcel_predictions.describe()
  print "Feasible square footage (in millions)"
  for col in parcel_predictions.columns: 
    print col, (parcel_predictions[col]*far_predictions.parcelsize).sum()/1000000.0
  parcel_predictions.to_csv('parcel_predictions.csv',index_col='parcel_id',float_format="%.2f")
  far_predictions.to_csv('far_predictions.csv',index_col='parcel_id',float_format="%.2f")
  print "Finished developer", time.ctime()

if __name__ == '__main__':  

  print "Running spotproforma"
  dev = spotproforma.Developer()
  print "Done running spotproforma"
  dset = baydataset.BayAreaDataset(os.path.join(misc.data_dir(),'bayarea.h5'))
  run(dset,2010,dev=dev)
