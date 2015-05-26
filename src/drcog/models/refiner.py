import os, numpy as np, pandas as pd
from synthicity.utils import misc

##Refines zone level model results

def run(dset, current_year):
    """Refines zone level model results
    """

    b = dset.buildings
    p = dset.parcels
    if p.index.name != 'parcel_id':
       p = p.set_index('parcel_id')

    z = dset.zones


    e = dset.establishments
    hh = dset.households
    zone_refine = pd.read_csv(os.path.join(misc.data_dir(),'zone_demand_refine.csv'))
    
    def relocate_agents(agents_joined,zone_id,number_of_agents):
        agent_pool = agents_joined[agents_joined.zone_id!=zone_id]
        shuffled_ids = agent_pool.index.values
        np.random.shuffle(shuffled_ids)
        agents_to_relocate = shuffled_ids[:number_of_agents]
        idx_agents_to_relocate = np.in1d(dset.households.index.values,agents_to_relocate)
        
        # new_building_id = b[b.zone_id==zone_id].index.values[0]
        # dset.households.building_id[idx_agents_to_relocate] = new_building_id
        try:
            new_building_id = b[b.zone_id==zone_id].index.values[0]
            agents_joined.building_id[idx_agents_to_relocate] = new_building_id
        except:
            print 'No buildings in specified zone.'
            if zone_id not in dset.parcels.zone_id.values:
                county = z.county.values[z.index.values==zone_id][0]
                x = z.zonecentroid_x.values[z.index.values==zone_id][0]
                y = z.zonecentroid_y.values[z.index.values==zone_id][0]
                if county == 'Denver':
                    county_id = 8031
                elif county == 'Adams':
                    county_id = 8001
                elif county == 'Arapahoe':
                    county_id = 8005
                elif county == 'Boulder':
                    county_id = 8013
                elif county == 'Broomfield':
                    county_id = 8014
                elif county == 'Clear Creek':
                    county_id = 8019
                elif county == 'Douglas':
                    county_id = 8035
                elif county == 'Elbert':
                    county_id = 8039
                elif county == 'Gilpin':
                    county_id = 8047
                elif county == 'Jefferson':
                    county_id = 8059
                elif county == 'Weld':
                    county_id = 8123
                pid = p.index.values.max()+1
                newparcel = pd.DataFrame({'county_id':[county_id],'parcel_sqft':[43560],'land_value':[0],'zone_id':[zone_id],
                                             'centroid_x':[x],'centroid_y':[y],'x':[x],'y':[y],'dist_bus':[6000],'dist_rail':[6000],'in_ugb':[1],'in_uga':[0],
                                             'prop_constrained':[0.0],'acres':[1.0] })
                newparcel.index = np.array([pid])
                dset.d['parcels'] = pd.concat([p,newparcel])
                dset.parcels.index.name = 'parcel_id'
            else:
                pid = p.index.values[p.zone_id==zone_id][0]
            print 'Constructing small structure to place agents'
            new_building_id = dset.buildings.index.values.max() + 1
            newbuildings = pd.DataFrame({'building_type_id':[20],'improvement_value':[10000],'land_area':[200],'non_residential_sqft':[0],
                                         'parcel_id':[pid],'residential_units':[2],'sqft_per_unit':[250],'stories':[0],'tax_exempt':[0],'year_built':[2000],'bldg_sq_ft':[500],
                                         'unit_price_non_residential':[0.0],'unit_price_residential':[5000.0], 'building_sqft_per_job':[0.0],
                                         'non_residential_units':[0],'base_year_jobs':[0.0],'all_units':[2]})
            newbuildings.index = np.array([new_building_id])
            dset.d['buildings'] = pd.concat([dset.buildings,newbuildings])
            dset.buildings.index.name = 'building_id'
            agents_joined.building_id[idx_agents_to_relocate] = new_building_id
            
    def unplace_agents(agents_joined,zone_id,number_of_agents):
        number_of_agents = -number_of_agents #flip the sign
        agent_pool = agents_joined[agents_joined.zone_id==zone_id] ##Notice the equality instead of disequality
        if len(agent_pool) > number_of_agents:
            shuffled_ids = agent_pool.index.values
            np.random.shuffle(shuffled_ids)
            agents_to_relocate = shuffled_ids[:number_of_agents]
            idx_agents_to_relocate = np.in1d(dset.households.index.values,agents_to_relocate)
            dset.households.building_id[idx_agents_to_relocate] = -1 #unplace
            
    def relocate_estabs(agents_joined,zone_id,number_of_agents):
        agent_pool = agents_joined[(agents_joined.zone_id!=zone_id)]
        e_sample = agent_pool.reindex(np.random.permutation(agent_pool.index))
        e_to_move = e_sample[np.cumsum(e_sample['employees'].values)<abs(number_of_agents+10)]
        shuffled_ids = e_to_move.index.values
        np.random.shuffle(shuffled_ids)
        agents_to_relocate = shuffled_ids
        idx_agents_to_relocate = np.in1d(dset.establishments.index.values,agents_to_relocate)
        
        # new_building_id = b[b.zone_id==zone_id].index.values[0]
        # dset.establishments.building_id[idx_agents_to_relocate] = new_building_id
        try:
            new_building_id = b[b.zone_id==zone_id].index.values[0]
            agents_joined.building_id[idx_agents_to_relocate] = new_building_id
        except:
            print 'No buildings in specified zone.'
            if zone_id not in dset.parcels.zone_id.values:
                county = z.county.values[z.index.values==zone_id][0]
                x = z.zonecentroid_x.values[z.index.values==zone_id][0]
                y = z.zonecentroid_y.values[z.index.values==zone_id][0]
                if county == 'Denver':
                    county_id = 8031
                elif county == 'Adams':
                    county_id = 8001
                elif county == 'Arapahoe':
                    county_id = 8005
                elif county == 'Boulder':
                    county_id = 8013
                elif county == 'Broomfield':
                    county_id = 8014
                elif county == 'Clear Creek':
                    county_id = 8019
                elif county == 'Douglas':
                    county_id = 8035
                elif county == 'Elbert':
                    county_id = 8039
                elif county == 'Gilpin':
                    county_id = 8047
                elif county == 'Jefferson':
                    county_id = 8059
                elif county == 'Weld':
                    county_id = 8123
                pid = p.index.values.max()+1
                newparcel = pd.DataFrame({'county_id':[county_id],'parcel_sqft':[43560],'land_value':[0],'zone_id':[zone_id],
                                             'centroid_x':[x],'centroid_y':[y],'x':[x],'y':[y],'dist_bus':[6000],'dist_rail':[6000],'in_ugb':[1],'in_uga':[0],
                                             'prop_constrained':[0.0],'acres':[1.0] })
                newparcel.index = np.array([pid])
                dset.d['parcels'] = pd.concat([p,newparcel])
                dset.parcels.index.name = 'parcel_id'
            else:
                pid = p.index.values[p.zone_id==zone_id][0]
            print 'Constructing small structure to place agents'
            new_building_id = dset.buildings.index.values.max() + 1
            newbuildings = pd.DataFrame({'building_type_id':[4],'improvement_value':[10000],'land_area':[200],'non_residential_sqft':[500],
                                         'parcel_id':[pid],'residential_units':[0],'sqft_per_unit':[0],'stories':[0],'tax_exempt':[0],'year_built':[2000],'bldg_sq_ft':[500],
                                         'unit_price_non_residential':[2.0],'unit_price_residential':[0.0], 'building_sqft_per_job':[250.0],
                                         'non_residential_units':[2],'base_year_jobs':[0.0],'all_units':[2]})
            newbuildings.index = np.array([new_building_id])
            dset.d['buildings'] = pd.concat([dset.buildings,newbuildings])
            dset.buildings.index.name = 'building_id'
            agents_joined.building_id[idx_agents_to_relocate] = new_building_id
            
    def unplace_estabs(agents_joined,zone_id,number_of_agents):
        number_of_agents = -number_of_agents #flip the sign
        agent_pool = agents_joined[agents_joined.zone_id==zone_id] ##Notice the equality instead of disequality
        if agent_pool.employees.sum() > number_of_agents:
            e_sample = agent_pool.reindex(np.random.permutation(agent_pool.index))
            e_to_move = e_sample[np.cumsum(e_sample['employees'].values)<abs(number_of_agents)]
            shuffled_ids = e_to_move.index.values
            np.random.shuffle(shuffled_ids)
            agents_to_relocate = shuffled_ids
            idx_agents_to_relocate = np.in1d(dset.establishments.index.values,agents_to_relocate)
            dset.establishments.building_id[idx_agents_to_relocate] = -1 #unplace
        
    for zone in zone_refine.zone_id.values:
        idx_zone = (zone_refine.zone_id==zone)
        hh_shift = zone_refine.annual_hh_shift[idx_zone].values[0]
        emp_shift = zone_refine.annual_emp_shift[idx_zone].values[0]
        if hh_shift > 0:
            relocate_agents(hh,zone,hh_shift)
        if emp_shift > 0:
            relocate_estabs(e,zone,emp_shift)
        if current_year < 2040:
            if hh_shift < 0:
                unplace_agents(hh,zone,hh_shift)
            if emp_shift < 0:
                unplace_agents(e,zone,emp_shift)
    