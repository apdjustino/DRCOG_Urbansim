import numpy as np, pandas as pd

def export_zonal_file_to_tm(dset,sim_year,tm_input_dir='C:\\urbansim\\data\\travel_model'):
    buildings = dset.fetch('buildings')[['building_type_id','improvement_value','land_area','non_residential_sqft','parcel_id','residential_units','sqft_per_unit','stories','tax_exempt','year_built','bldg_sq_ft','unit_price_non_residential','unit_price_residential','building_sqft_per_job','non_residential_units','base_year_jobs','all_units']]
    establishments = dset.fetch('establishments')
    del establishments['zone_id']
    del establishments['county_id']
    households = dset.fetch('households')
    del households['zone_id']
    del households['county_id']
    parcels = dset.fetch('parcels')
    zones = dset.fetch('zones')
    pz = pd.merge(parcels.reset_index(),zones,left_on='zone_id',right_index=True,how='left')
    pz = pz.set_index('parcel_id')
    bpz = pd.merge(buildings,pz,left_on='parcel_id',right_index=True)
    
    ##Merge buildings and parcels
    buildings = pd.merge(buildings,parcels,left_on='parcel_id',right_index=True)

    ##Merge households with bulidings/parcels
    households = pd.merge(households,buildings,left_on='building_id',right_index=True)

    ##Merge establishments with bulidings/parcels
    establishments = pd.merge(establishments,buildings,left_on='building_id',right_index=True)
    
    ##Calculate the income breakpoints
    low_income_breakpoint = households.income.quantile(.11)
    high_income_breakpoint = households.income.quantile(.75)

    ##Tag households according to income bucket
    households['low_inc_hh'] = households.income<low_income_breakpoint
    households['med_inc_hh'] = (households.income>=low_income_breakpoint)*(households.income<high_income_breakpoint)
    households['high_inc_hh'] = households.income>=high_income_breakpoint
    
    ##Zonal household variables
    hh_pop = households.groupby('zone_id').persons.sum()
    avg_hh_size = households.groupby('zone_id').persons.mean()
    tot_hh = households.groupby('zone_id').size()
    low_inc_hh = households[households.low_inc_hh==True].groupby('zone_id').size()
    med_inc_hh = households[households.med_inc_hh==True].groupby('zone_id').size()
    high_inc_hh = households[households.high_inc_hh==True].groupby('zone_id').size()
    
    ##Zonal establishment variables
    total_employment = establishments.groupby('zone_id').employees.sum()
    prod_dist_sectors = [11,21,22,23,31,32,33,42,48,49]
    prod_sectors = [11,21,22,23,31,32,33,42]
    retail_sectors = [44,45,7211,7212,7213,7223]
    retail2_sectors = [44,45,7211,7212,7213,7223,7221,7222,7224]
    restaurant_sectors = [7221,7222,7224]
    service_sectors = [51,52,53,54,55,56,62,81,92]
    service2_sectors = [48,49,51,52,53,54,55,56,62,81,92]
    prod_dist_emp = establishments[np.in1d(establishments.sector_id,prod_dist_sectors)].groupby('zone_id').employees.sum()
    retail_emp = establishments[np.in1d(establishments.sector_id,retail_sectors)].groupby('zone_id').employees.sum()
    service_emp = establishments[np.in1d(establishments.sector_id,service_sectors)].groupby('zone_id').employees.sum()

    education_emp = establishments[establishments.sector_id==61].groupby('zone_id').employees.sum()
    entertainment_emp = establishments[establishments.sector_id==71].groupby('zone_id').employees.sum()
    production_emp = establishments[np.in1d(establishments.sector_id,prod_sectors)].groupby('zone_id').employees.sum()
    restaurant_emp = establishments[np.in1d(establishments.sector_id,restaurant_sectors)].groupby('zone_id').employees.sum()
    retail2_emp = establishments[np.in1d(establishments.sector_id,retail2_sectors)].groupby('zone_id').employees.sum()
    service2_emp = establishments[np.in1d(establishments.sector_id,service2_sectors)].groupby('zone_id').employees.sum()
    
    tm_export = pd.DataFrame(index=zones.index)
    tm_export['HH_Pop'] = hh_pop
    tm_export['Low_Inc_HH'] = low_inc_hh
    tm_export['Med_Inc_HH'] = med_inc_hh
    tm_export['High_Inc_HH'] = high_inc_hh
    tm_export['TOT_HH'] = tot_hh
    tm_export['AVG_HH_Size'] = avg_hh_size
    tm_export['Prod_Dist_Emp'] = prod_dist_emp
    tm_export['Retail_Emp'] = retail_emp
    tm_export['Service_Emp'] = service_emp
    tm_export['TotalEmployment'] = total_employment
    tm_export['Education_25'] = education_emp
    tm_export['Entertainment_25'] = entertainment_emp
    tm_export['Production_25'] = production_emp
    tm_export['restaurant_25'] = restaurant_emp
    tm_export['Retail_25'] = retail2_emp
    tm_export['Service_25'] = service2_emp

    tm_export = tm_export.fillna(0)

    fixed_vars = pd.read_csv('C:\\urbansim\\data\\travel_model\\fixed.csv')
    variable_vars = pd.read_csv('C:\\urbansim\\data\\travel_model\\variable.csv')

    tm_export = pd.merge(fixed_vars,tm_export,left_on='ZoneID',right_index=True)
    tm_export = pd.merge(tm_export,variable_vars,left_on='ZoneID',right_on='ZoneID')
    
    tm_export.to_csv(tm_input_dir+'\\ZonalDataTemplate%s.csv'%sim_year,index=False)
    
    #####Export jobs table
    e = establishments.reset_index()
    bids = []
    eids = []
    hbs = []
    sids = []
    for idx in e.index:
        for job in range(e.employees[idx]):
            bids.append(e.building_id[idx])
            eids.append(e.index[idx])
            hbs.append(e.home_based_status[idx])
            sids.append(e.sector_id[idx])
    print len(bids)
    print len(eids)
    print len(hbs)
    print len(sids)
    jobs = pd.DataFrame({'job_id':range(1,len(bids)+1),'building_id':bids,'establishment_id':eids,'home_based_status':hbs,'sector_id':sids})
    jobs['x'] = bpz.centroid_x[jobs.building_id].values
    jobs['y'] = bpz.centroid_y[jobs.building_id].values
    jobs['taz05_id'] = bpz.external_zone_id[jobs.building_id].values
    jobs['sector_id_six'] = 1*(jobs.sector_id==61) + 2*(jobs.sector_id==71) + 3*np.in1d(jobs.sector_id,[11,21,22,23,31,32,33,42,48,49]) + 4*np.in1d(jobs.sector_id,[7221,7222,7224]) + 5*np.in1d(jobs.sector_id,[44,45,7211,7212,7213,7223]) + 6*np.in1d(jobs.sector_id,[51,52,53,54,55,56,62,81,92])
    jobs['jobtypename'] = ''
    jobs.jobtypename[jobs.sector_id_six==1] = 'Education'
    jobs.jobtypename[jobs.sector_id_six==2] = 'Entertainment'
    jobs.jobtypename[jobs.sector_id_six==3] = 'Production'
    jobs.jobtypename[jobs.sector_id_six==4] = 'Restaurant'
    jobs.jobtypename[jobs.sector_id_six==5] = 'Retail'
    jobs.jobtypename[jobs.sector_id_six==6] = 'Service'
    jobs['urbancenter_id'] = 0
    del jobs['sector_id_six']
    jobs.to_csv(tm_input_dir+'\\jobs%s.csv'%sim_year,index=False)
    
    #####Export synthetic households
    h = households[['age_of_head','building_id','cars','children','county','income','income_group_id','persons','race_id','tenure','workers']]
    h.to_csv(tm_input_dir+'\\SynHH%s.csv'%sim_year,index=False)
    
    
    #####################
    #####Other indicators
    #####################
    other_zonal_indicators = pd.DataFrame(index=zones.index)
    other_county_indicators = pd.DataFrame(index=np.unique(parcels.county_id))
    
    ##Number of children in zone
    children = households.groupby('zone_id').children.sum()
    
    ##Median household income in zone
    med_hh_inc = households.groupby('zone_id').income.median()
    
    ##Average household income in zone
    avg_hh_inc = households.groupby('zone_id').income.mean()
    
    ##Number of hh workers by zone
    hh_workers = households.groupby('zone_id').workers.sum()
    
    ##Number of cars in county
    cars = households.groupby('county_id').cars.sum()
    
    ##Employment by county
    emp_by_county = establishments.groupby('county_id').employees.sum()
    
    ##Agricultural jobs in county
    ag_jobs = establishments[establishments.sector_id==11].groupby('county_id').employees.sum()
    
    ##Residential units by county
    resunits = buildings.groupby('county_id').residential_units.sum()
    
    other_zonal_indicators['children'] = children
    other_zonal_indicators['med_hh_inc'] = med_hh_inc
    other_zonal_indicators['avg_hh_inc'] = avg_hh_inc
    other_zonal_indicators['hhworkers_by_zone'] = hh_workers
    
    other_county_indicators['cars'] = cars
    other_county_indicators['county_employment'] = emp_by_county
    other_county_indicators['agricultural_jobs'] = ag_jobs
    
    other_zonal_indicators.to_csv(tm_input_dir+'\\other_zonal_indicators%s.csv'%sim_year)
    other_county_indicators.to_csv(tm_input_dir+'\\other_county_indicators%s.csv'%sim_year)
    