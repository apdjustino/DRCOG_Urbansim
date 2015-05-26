import numpy as np, pandas as pd, time
from numpy import array as arr
pd.set_option('display.precision',3)

# these are the parcel sizes we test, turns out nothing is dependent on size right now
parcelsizes = np.array([10000.0])
# these ar ethe fars we test
fars = np.array([.1,.25,.5,.75,1.0,1.5,2.0,3.0,4.0,5.0,7.0,9.0,11.0])

# these are the uses we test and the mixes (forms) of those uses
uses = ['retail','industrial','office','residential']
forms = {
    'retail':           arr([1.0,0.0,0.0,0.0]),
    'industrial':       arr([0.0,1.0,0.0,0.0]),
    'office':           arr([0.0,0.0,1.0,0.0]),
    'residential':      arr([0.0,0.0,0.0,1.0]),
    #'mixedresidential': arr([0.1,0.0,0.0,0.9]),
    #'mixedoffice':      arr([0.0,0.0,0.7,0.3])  ####Turn back on if I can find a DRCOG btype to map this to...
    }

#PROFITFACTOR = 1.1 # ratio times break even rent.  The lower this is, the more gets constructed.
EFFICIENCY = .8 # interior building efficient
PARCELUSE = .8 # efficiency of footprint on parcel
#INTERESTRATE = .05 # interest rate
INTERESTRATE =1.0
PERIODS = 20 # number of periods (years)

parking_rates = arr([2.0,.8,1.0,1.2]) # per uses above and per thousands of sqft
SQFTPERRATE = 1000.0

parking_configs = ['surface','deck','underground']

"""
costs = np.transpose(np.array([ \
[80.0,110.0,120.0,130.0], #retail
[80.0,110.0,120.0,130.0], #industrial
[100.0,140.0,200.0,250.0], # office
[120.0,140.0,200.0,250.0], # multifamily
]))
"""
costs = np.transpose(np.array([ \
[80.0,110.0,120.0,130.0], #retail
[80.0,110.0,120.0,130.0], #industrial
[100.0,140.0,200.0,250.0], # office
[120.0,140.0,200.0,250.0], # multifamily
]))

costs = costs*.9 #Decreasing costs relative to the Bay
heightsforcosts = [15,55,120,np.inf] # max height for each of the costs above from left to right, same for all uses

# parking costs!
parking_sqft_d = {'surface':300.0, 'deck':250.0, 'underground':250.0}
parking_cost_d = {'surface':30, 'deck':90, 'underground':110}

HEIGHTPERSTORY = 12.0
MAXRETAILHEIGHT = 2.0
MAXINDUSTRIALHEIGHT = 2.0

def building_cost(use_mix,stories,df):
  height = stories*HEIGHTPERSTORY

  df['stories'] = stories

  cost = np.searchsorted(heightsforcosts,height)
  cost[np.isnan(height)] = 0

  cost = np.dot(np.squeeze(costs[cost.astype('int32')]),use_mix)
  cost[np.isnan(stories).flatten()] = np.nan

  cost = np.reshape(cost,(-1,1))
  df['costsqft'] = cost

  return cost

tiledparcelsizes = np.reshape(np.repeat(parcelsizes,fars.size),(-1,1))

class Developer:
 PROFITFACTOR = 1.4
 def __init__(self,profit_factor=1.0):
  self.generate_lookup()
  self.PROFITFACTOR = self.PROFITFACTOR*profit_factor
  np.random.seed(1)
 # run the developer model on all parcel inputs (not actually running on parcels here)
 # these are hypothetical buildings
 def generate_lookup(self):
  keys = forms.keys()
  keys.sort()
  df_d = {}
  for name in keys:
    uses_distrib = forms[name]

    for parking_config in parking_configs:

      df = pd.DataFrame(index=fars)
      df['far'] = fars
      df['pclsz'] = tiledparcelsizes

      building_bulk = np.reshape(parcelsizes,(-1,1))*np.reshape(fars,(1,-1))
      building_bulk = np.reshape(building_bulk,(-1,1))

      if parking_config == 'deck': # need to converge in on exactly how much far is available for deck pkg
        orig_bulk = building_bulk
        while 1:
          parkingstalls = building_bulk*np.sum(uses_distrib*parking_rates)/SQFTPERRATE
          if np.where(np.absolute(orig_bulk-building_bulk-parkingstalls*parking_sqft_d[parking_config]) > 10.0)[0].size == 0: break
          building_bulk = orig_bulk - parkingstalls*parking_sqft_d[parking_config]

      df['build'] = building_bulk

      parkingstalls = building_bulk*np.sum(uses_distrib*parking_rates)/SQFTPERRATE
      parking_cost = parking_cost_d[parking_config]*parkingstalls*parking_sqft_d[parking_config]

      df['spaces'] = parkingstalls

      if parking_config == 'underground': 
        df['parksqft'] = parkingstalls*parking_sqft_d[parking_config]
        stories = building_bulk / tiledparcelsizes
      if parking_config == 'deck': 
        df['parksqft'] = parkingstalls*parking_sqft_d[parking_config]
        stories = (building_bulk+parkingstalls*parking_sqft_d[parking_config]) / tiledparcelsizes
      if parking_config == 'surface': 
        stories = building_bulk / (tiledparcelsizes-parkingstalls*parking_sqft_d[parking_config]) 
        df['parksqft'] = parkingstalls*parking_sqft_d[parking_config]
        stories[np.where(stories < 0.0)] = np.nan

      stories /= PARCELUSE;
      cost = building_cost(uses_distrib,stories,df)*building_bulk+parking_cost # acquisition cost!
      df['parkcost'] = parking_cost
      df['cost'] = cost

      yearly_cost_per_sqft = np.pmt(INTERESTRATE,PERIODS,cost)/(building_bulk*EFFICIENCY)
      df['yearly_pmt'] = yearly_cost_per_sqft

      break_even_weighted_rent = self.PROFITFACTOR*yearly_cost_per_sqft*-1.0
      if name == 'retail': break_even_weighted_rent[np.where(fars>MAXRETAILHEIGHT)] = np.nan
      if name == 'industrial': break_even_weighted_rent[np.where(fars>MAXINDUSTRIALHEIGHT)] = np.nan
      #df['even_rent'] = break_even_weighted_rent
      df['even_rent'] = building_cost(uses_distrib,stories,df)



      df_d[(name,parking_config)] = df

  self.df_d = df_d

  min_even_rents_d = {}
  BIG = 999999

  for name in keys:
    min_even_rents = None
    for parking_config in parking_configs:
        even_rents = df_d[(name,parking_config)]['even_rent'].fillna(BIG)
        min_even_rents = even_rents if min_even_rents is None else np.minimum(min_even_rents,even_rents)

    min_even_rents = min_even_rents.replace(BIG,np.nan)
    min_even_rents_d[name] = min_even_rents # this is the minimum cost per sqft for this form and far

  self.min_even_rents_d = min_even_rents_d

 # this function does the developer model lookups for all the actual parcels
 # form must be one of the forms specified here
 # rents is a matrix of rents of shape (numparcels x numuses)
 # land_costs is the current yearly rent on each parcel
 # parcel_size is the size of the parcel

 def lookup(self,form,rents,land_costs,parcel_sizes):

  print form, time.ctime()

  rents = np.dot(rents,forms[form]) # get weighted rent for this form
  print rents.mean()

  even_rents = self.min_even_rents_d[form]
  # print "sqft cost\n", even_rents

  building_bulks = np.reshape(parcel_sizes,(-1,1))*np.reshape(even_rents.index,(1,-1)) # parcel sizes * possible fars

  building_costs = building_bulks * np.reshape(even_rents.values,(1,-1)) / INTERESTRATE # cost to build the new building

  building_costs += np.reshape(land_costs.values,(-1,1)) / INTERESTRATE # add cost to buy the current building

  building_revenue = building_bulks * np.reshape(rents,(-1,1)) / INTERESTRATE # rent to make for the new building


  profit = building_revenue.values - building_costs.values # profit for each form


  maxprofitind = np.argmax(profit,axis=1) # index maximum total profit

  maxprofit = profit[np.arange(maxprofitind.size),maxprofitind] # value of the maximum total profit

  maxprofit_fars = pd.Series(even_rents.index[maxprofitind].astype('float'),index=parcel_sizes.index) # far of the max profit

  maxprofit = pd.Series(maxprofit.astype('float'),index=parcel_sizes.index)
  maxprofit.values[maxprofit<0] = np.nan # remove unprofitable buildings
  maxprofit_fars.values[np.isnan(maxprofit)] = np.nan  # remove far of unprofitable building


  # print maxprofit_fars.value_counts()

  return maxprofit_fars.astype('float32'), maxprofit


 def profit(self, form, rents, land_costs, parcel_sizes):


  rents = np.dot(rents,forms[form])
  even_rents = self.min_even_rents_d[form]
  building_bulks = np.reshape(parcel_sizes,(-1,1))
  buidling_revenue=building_bulks*np.reshape(rents, (-1,1))
  building_costs=building_bulks*np.reshape(even_rents.values, (-1,1))+ np.reshape(land_costs.values,(-1,1))

  return np.max(buidling_revenue-building_costs,0)

 # this code creates the debugging plots to understand the behavior of
 # the hypothetical building model
 def debug_output(self):
  import matplotlib.pyplot as plt

  df_d = self.df_d
  keys = df_d.keys()
  keys.sort()
  for key in keys:
    print "\n", key, "\n"
    print df_d[key]
  for key in self.min_even_rents_d.keys():
    print "\n", key, "\n"
    print self.min_even_rents_d[key]

  keys = forms.keys()
  keys.sort()
  c = 1
  share = None
  fig = plt.figure(figsize=(12,3*len(keys)))
  fig.suptitle('Profitable rents by use',fontsize=40)
  for name in keys:
    sumdf = None
    for parking_config in parking_configs:
      df = df_d[(name, parking_config)]
      if sumdf is None: 
        sumdf = pd.DataFrame(df['far'])
      sumdf[parking_config] = df['even_rent']
    far = sumdf['far']
    del sumdf['far']

    if share is None: share = plt.subplot(len(keys)/2,2,c) 
    else: plt.subplot(len(keys)/2,2,c,sharex=share,sharey=share) 

    handles = plt.plot(far,sumdf)
    plt.ylabel('even_rent')
    plt.xlabel('FAR')
    plt.title('Rents for use type %s'%name)
    plt.legend(handles,parking_configs,loc='lower right',title='Parking type')
    c += 1
  plt.savefig('even_rents.png',bbox_inches=0)

if __name__ == '__main__':  
  dev = Developer()
  dev.debug_output()
