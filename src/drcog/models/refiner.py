import os, numpy as np, pandas as pd
from synthicity.utils import misc

##Refines zone level model results

def run(dset, current_year):
    """Refines zone level model results
    """

    b = dset.buildings
    e = dset.establishments
    hh = dset.households
    zone_refine = pd.read_csv(os.path.join(misc.data_dir(),'zone_demand_refine.csv'))
    
    def relocate_agents(agents_joined,zone_id,number_of_agents):
        agent_pool = agents_joined[agents_joined.zone_id!=zone_id]
        shuffled_ids = agent_pool.index.values
        np.random.shuffle(shuffled_ids)
        agents_to_relocate = shuffled_ids[:number_of_agents]
        idx_agents_to_relocate = np.in1d(dset.households.index.values,agents_to_relocate)
        
        new_building_id = b[b.zone_id==zone_id].index.values[0]
        dset.households.building_id[idx_agents_to_relocate] = new_building_id
        # try:
            # new_building_id = b[b.zone_id==zone_id].index.values[0]
            # agents_joined.building_id[idx_agents_to_relocate] = new_building_id
        # except:
            # print 'No buildings in specified zone.  Cannot place agents.'
            
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
        
        new_building_id = b[b.zone_id==zone_id].index.values[0]
        dset.establishments.building_id[idx_agents_to_relocate] = new_building_id
        # try:
            # new_building_id = b[b.zone_id==zone_id].index.values[0]
            # agents_joined.building_id[idx_agents_to_relocate] = new_building_id
        # except:
            # print 'No buildings in specified zone.  Cannot place agents.'
            
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
    