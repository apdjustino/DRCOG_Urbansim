import pandas as pd, numpy as np
import sys, time, random, string, os
from synthicity.utils import misc
#import dataset
np.random.seed(1)
SHIFTAMOUNT = 1.0 # if demand exceeds supply
RESUNITSIZE = 1500 # should probably vary, but is currently set to 1000
BSQFT_JOB = 400
# MAXLOTSIZE = 500000 # 5 acres-ish - limit size of largest development ## I changed the 2 to a 5, so max acres is now ~12.5
# MINLOTSIZE = 100
#BUILDINGTYPEORDER = [12,3,2,1,14,4,10,11,7,8,9]
#BUILDINGTYPEORDER = [3,2,24,20,11,5,17,18,9,22] #DRCOG, including mixed use
#BUILDINGTYPEORDER = [3,2,24,20,5,17,18,9,22]
BUILDINGTYPEORDER = [5,17,18,9,22,3,2,24,20]
number_of_zones = 2804

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

# TODO get the real demand here
def get_simulated_demand(btyp,zone_id,hh_zone_diff,emp_zone_diff,zone_args):  ###Expressed in terms of sqft
  #return np.random.rand()*20000 # 2000 sqft in every zone!
  if (btyp in [2,3,20,24]) and (zone_id in hh_zone_diff.index):
      hh_diff = hh_zone_diff[zone_id]
      if zone_args is not None:
          subsidized_demand = zone_args.subsidized_hh_demand[zone_id]
          hh_diff = hh_diff + subsidized_demand
      if hh_diff > 0:
          # print 'hh_diff'
          # print hh_diff
          #return hh_diff*RESUNITSIZE
          return hh_diff*300
  elif (btyp in [5,17,18,9,22]) and (zone_id in emp_zone_diff.index):
      emp_diff = emp_zone_diff[zone_id]

      if zone_args is not None:
          subsidized_demand = zone_args.subsidized_emp_demand[zone_id]
          emp_diff = emp_diff
                     #+ subsidized_demand
      if emp_diff > 0:
          # print 'emp_diff'
          # print emp_diff
          #return emp_diff*BSQFT_JOB
          return emp_diff*50
  else:
      #return 1000
      #print 'No %s demand in zone %s.' % (btyp,zone_id)
      pass
      

price_shifters = {}
def shift_up(btyp,zone_id):
  key = (btyp,zone_id)
 # price_shifters[key] = price_shifters.get(key,1.0)*SHIFTAMOUNT

def run(dset,hh_zone_diff,emp_zone_diff,parcel_predictions,year=2010,min_building_sqft=400,min_lot_sqft=250,max_lot_sqft=500000,zone_args=None):

  np.random.seed(1)
  MINBUILDINGSQFT = min_building_sqft
  MAXLOTSIZE = max_lot_sqft
  MINLOTSIZE = min_lot_sqft
  #print os.getcwd()

#parcel_predictions = pd.read_csv(os.path.join(misc.data_dir(),'parcel_predictions.csv'),index_col='parcel_id') # feasible buildings
  parcels=dset.parcels
  if parcels.index.name !='parcel_id':
      parcels = parcels.set_index(parcels['parcel_id'])
  parcel_predictions['zone_id'] = parcels.zone_id.ix[parcel_predictions.index]



  newbuildings_d = {}
  for btyp in BUILDINGTYPEORDER:
    newbuildings = []
    btyp_predictions = parcel_predictions[btyp].dropna()
    #btyp_pro=parcel_predictions[btyp+ '_profit'].dropna()


    for zone_id in range(1,number_of_zones+1):
      if zone_args is not None:
        if zone_args.no_build[zone_id] == 1:
            continue

      # get the demands
      demand_sqft = get_simulated_demand(btyp,zone_id,hh_zone_diff,emp_zone_diff,zone_args)
      """
      if not demand_sqft:
          print "unutilized capacity " + str(btyp_predictions[parcel_predictions.zone_id == zone_id].sum())
          continue
    """
      # get the feasible buildings for this building type and zone_id
      choiceset = btyp_predictions[parcel_predictions.zone_id == zone_id]

      if len(choiceset.index) == 0:
        if demand_sqft > 3000:
          # raise prices
          shift_up(btyp,zone_id)  ##Needs to be implemented, but this can be a later stage thing
        continue
      #choiceset = choiceset*dset.parcels.ix[choiceset.index].shape_area*10.764 # convert to sqft
      choiceset = choiceset*parcels.ix[choiceset.index].parcel_sqft
      choiceset[choiceset<MINBUILDINGSQFT] = MINBUILDINGSQFT


      #print "capacity " +str(choiceset.sum()) +" versus demand " + str(demand_sqft) + " in zone " + str(zone_id)

      if not demand_sqft:
          #print "unutilized capacity " + str(choiceset.sum()) + " in zone " + str(zone_id)
          continue
      #print "capacity " +str(choiceset.sum()) +" versus demand " + str(demand_sqft) + " in zone " + str(zone_id)
      # weight the choice by the FAR, which presumably is directly related to profitability
      # XG: this would be somewhat true if there were no constraint on the fars. But here, not true at all, because of constraints on fars.
      choiceset_ind = np.random.choice(choiceset.index,len(choiceset.index),replace=False,p=choiceset.values/choiceset.sum())
      """
      # pick the first X number of buildings in order to meet demand
      choiceset_sorted=np.sort(choiceset)
      choiset_sorted=choiceset_sorted[::-1]
      #buildthese = np.searchsorted(choiceset.ix[choiceset_ind].cumsum(),demand_sqft) # round up
      buildthese=np.searchsorted(choiceset_sorted.cumsum(), demand_sqft)
    """

      accu_built=0
      choice=np.zeros(len(choiceset_ind))
      i=0
      while (accu_built<demand_sqft):
          if (choiceset_ind.size>0):
            choice[i]=choiceset_ind[0]
            accu_built=accu_built+choiceset[choiceset_ind[0]]
            choiceset_ind=choiceset_ind[1:len(choiceset_ind)]
            i=i+1
          else:

            break
      choice=choice[choice>0]



      #if buildthese > choiceset_ind.size:
        # raise prices
        #lack=(demand_sqft-choiceset.ix[choiceset_ind].sum())/50
        #print "Capacity issue in zone " + str(zone_id)  +" of " + str(lack)
        #hift_up(btyp,zone_id)  ##Needs to be implemented, but this can be a later stage thing.  H
      #newbuildings.append(choiceset.ix[choiceset_ind[:buildthese]])

      newbuildings.append(choiceset.ix[choice])


      ## XG: in case of no new building
    try:
        newbuildings=pd.concat(newbuildings)
        parcel_predictions = parcel_predictions.drop(newbuildings.index)
        newbuildings_d[btyp] = newbuildings
        print "%d new buildings for btyp %d" % (len(newbuildings.index), btyp)
    except:
        print "%d new buildings for btyp %d" % (0, btyp)



  newbuildings = pd.DataFrame(newbuildings_d)
  newbuildings = newbuildings.stack().reset_index(level=1)
  newbuildings.columns = ["building_type_id","building_sqft"]
  """

  newbuildings['parcel_id']=newbuildings.index
  newbuildings['county_id']=parcel_predictions0.county_id[newbuildings.parcel_id]
  for var in list(newbuildings.columns.values):
    print newbuildings.groupby('county_id')[var].sum()
    """
  
  newbuildings['general_type'] = newbuildings['building_type_id'].map(inv_type_d)
  #newbuildings['lot_size'] = dset.parcels.ix[newbuildings.index].shape_area*10.764
  newbuildings['lot_size'] = dset.parcels.ix[newbuildings.index].parcel_sqft
  newbuildings['residential_units'] = np.ceil((newbuildings.general_type=='Residential')*newbuildings.building_sqft/RESUNITSIZE)
  newbuildings = newbuildings[newbuildings.lot_size<MAXLOTSIZE]
  newbuildings = newbuildings[newbuildings.lot_size>MINLOTSIZE]
  
  newbuildings['net_residential_units'] = newbuildings.residential_units.sub(
               dset.buildings.groupby('parcel_id').residential_units.sum().ix[newbuildings.index],fill_value=0)
  print newbuildings.describe()
  print newbuildings.groupby('building_type_id').building_sqft.sum()
  print newbuildings.groupby('building_type_id').residential_units.sum()
  #newbuildings.to_csv('new_buildings.csv')
  return newbuildings, price_shifters
  # XXX need to remove building that existed on these parcels 
  # before and add these buildings to the list of buildings

if __name__ == '__main__':  

  #dset = baydataset.BayAreaDataset(os.path.join(misc.data_dir(),'bayarea.h5'))
  import dataset
  dset = dataset.DRCOGDataset(os.path.join(misc.data_dir(),'drcog.h5'))
  run(dset,2010)
