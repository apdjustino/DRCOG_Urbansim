import numpy as np, pandas as pd
import time, os
from synthicity.urbansim import dataset

class DRCOGDataset(dataset.Dataset):

  def __init__(self,filename):
    super(DRCOGDataset,self).__init__(filename)
  
  def fetch_annual_job_relocation_rates(self):
    return self.store["annual_job_relocation_rates"].reset_index()
  
  def fetch_annual_employment_control_totals(self):
    return self.store["annual_employment_control_totals"]
    #return self.store["annual_employment_control_totals"].reset_index(level=2).reset_index(level=0)
    
  def fetch_target_vacancies(self):
    return self.store["target_vacancies"].stack().reset_index(level=2,drop=True).unstack(level=0)

  def fetch_buildings(self,building_sqft_per_job_table='building_sqft_per_job'):
    # this is a bug in the data - need to eliminate duplicate ids - not allowed!
    buildings = self.store['buildings'].reset_index().drop_duplicates().set_index('building_id')
    buildings["zone_id"] = self.parcels["zone_id"][buildings.parcel_id].values
    buildings = pd.merge(buildings,self.store[building_sqft_per_job_table],left_on=['zone_id','building_type_id'],right_index=True,how='left')
    buildings["non_residential_units"] = buildings.non_residential_sqft/buildings.building_sqft_per_job#####
    buildings["base_year_jobs"] = self.establishments.groupby('building_id').employees.sum()
    # things get all screwed up if you have overfull buildings
    buildings["non_residential_units"] = buildings[["non_residential_units","base_year_jobs"]].max(axis=1)
    buildings["all_units"] = buildings.residential_units + buildings.non_residential_units
    return buildings

  def compute_range(self,attr,dist,agg=np.sum):
    travel_data = self.fetch('travel_data').reset_index(level=1)
    travel_data = travel_data[travel_data.am_single_vehicle_to_work_travel_time<dist]
    travel_data["attr"] = attr[travel_data.to_zone_id].values
    return travel_data.groupby(level=0).attr.apply(agg) 