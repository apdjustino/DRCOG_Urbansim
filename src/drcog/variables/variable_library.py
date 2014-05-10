import pandas as pd, numpy as np
elcm_configuration = {'building_sqft_per_job_table':'building_sqft_per_job','scaling_factor':1.0}

#VARIABLE LIBRARY

def calculate_variables(dset):

    ##PARCEL VARIABLES
    p = dset.fetch('parcels')
    if p.index.name != 'parcel_id':
        p = p.set_index('parcel_id')
    p['in_denver'] = (p.county_id==8031).astype('int32')
    p['ln_dist_rail'] = p.dist_rail.apply(np.log1p)
    p['ln_dist_bus'] = p.dist_bus.apply(np.log1p)
    p['ln_land_value'] = p.land_value.apply(np.log1p)
    p['land_value_per_sqft'] = p.land_value*1.0/p.parcel_sqft
    p['rail_within_mile'] = (p.dist_rail<5280).astype('int32')
    p['cherry_creek_school_district'] = (p.school_district==8).astype('int32')
    p['acres'] = p.parcel_sqft/43560.0
    p['ln_acres'] = (p.parcel_sqft/43560.0).apply(np.log1p)
    
    #BUILDING VARIABLES
    b = dset.fetch('buildings',building_sqft_per_job_table=elcm_configuration['building_sqft_per_job_table'],bsqft_job_scaling=elcm_configuration['scaling_factor'])
    b = b[['building_type_id','improvement_value','land_area','non_residential_sqft','parcel_id','residential_units','sqft_per_unit','stories','tax_exempt','year_built','bldg_sq_ft','unit_price_non_residential','unit_price_residential','building_sqft_per_job','non_residential_units','base_year_jobs','all_units']]
    b['zone_id'] = p.zone_id[b.parcel_id].values
    b['county_id'] = p.county_id[b.parcel_id].values
    b['townhome'] = (b.building_type_id==24).astype('int32')
    b['multifamily'] = (np.in1d(b.building_type_id,[2,3])).astype('int32')
    b['office'] = (b.building_type_id==5).astype('int32')
    b['retail_or_restaurant'] = (np.in1d(b.building_type_id,[17,18])).astype('int32')
    b['industrial_building'] = (np.in1d(b.building_type_id,[9,22])).astype('int32')
    b['btype_hlcm'] = 1*(b.building_type_id==2) + 2*(b.building_type_id==3) + 3*(b.building_type_id==20) + 4*np.invert(np.in1d(b.building_type_id,[2,3,20]))
    b['county8001'] = (b.county_id==8001).astype('int32')
    b['county8005'] = (b.county_id==8005).astype('int32')
    b['county8013'] = (b.county_id==8013).astype('int32')
    b['county8014'] = (b.county_id==8014).astype('int32')
    b['county8019'] = (b.county_id==8019).astype('int32')
    b['county8031'] = (b.county_id==8031).astype('int32')
    b['county8035'] = (b.county_id==8035).astype('int32')
    b['county8039'] = (b.county_id==8039).astype('int32')
    b['county8047'] = (b.county_id==8047).astype('int32')
    b['county8059'] = (b.county_id==8059).astype('int32')
    b['county8123'] = (b.county_id==8123).astype('int32')
    p['nonres_far'] = (b.groupby('parcel_id').non_residential_sqft.sum()/p.acres).apply(np.log1p)
    p['ln_units_per_acre'] = (b.groupby('parcel_id').residential_units.sum()/p.acres).apply(np.log1p)
    
    #HOUSEHOLD VARIABLES
    hh_estim = dset.fetch('households_for_estimation')
    hh_estim['tenure'] = 1
    hh_estim.tenure[hh_estim.own>1] = 2
    hh_estim['income']=0
    hh_estim.income[hh_estim.income_group==1] = 7500
    hh_estim.income[hh_estim.income_group==2] = 17500
    hh_estim.income[hh_estim.income_group==3] = 25000
    hh_estim.income[hh_estim.income_group==4] = 35000
    hh_estim.income[hh_estim.income_group==5] = 45000
    hh_estim.income[hh_estim.income_group==6] = 55000
    hh_estim.income[hh_estim.income_group==7] = 67500
    hh_estim.income[hh_estim.income_group==8] = 87500
    hh_estim.income[hh_estim.income_group==9] = 117500
    hh_estim.income[hh_estim.income_group==10] = 142500
    hh_estim.income[hh_estim.income_group==11] = 200000
    hh = dset.fetch('households')
    for table in [hh_estim, hh]:
        choosers = table
        choosers['zone_id'] = b.zone_id[choosers.building_id].values
        choosers['building_type_id'] = b.building_type_id[choosers.building_id].values
        choosers['county_id'] = b.county_id[choosers.building_id].values
        choosers['btype'] = 1*(choosers.building_type_id==2) + 2*(choosers.building_type_id==3) + 3*(choosers.building_type_id==20) + 4*np.invert(np.in1d(choosers.building_type_id,[2,3,20]))
        choosers['income_3_tenure'] = 1 * (choosers.income < 60000)*(choosers.tenure == 1) + 2 * np.logical_and(choosers.income >= 60000, choosers.income < 120000)*(choosers.tenure == 1) + 3*(choosers.income >= 120000)*(choosers.tenure == 1) + 4*(choosers.income < 40000)*(choosers.tenure == 2) + 5*(choosers.income >= 40000)*(choosers.tenure == 2)
        choosers['younghead'] = choosers.age_of_head<30
        choosers['hh_with_child'] = choosers.children>0
        choosers['ln_income'] = choosers.income.apply(np.log1p)
        choosers['income5xlt'] = choosers.income*5.0
        choosers['income10xlt'] = choosers.income*5.0
        choosers['wkrs_hhs'] = choosers.workers*1.0/choosers.persons
        
    #ESTABLISHMENT VARIABLES
    e = dset.fetch('establishments')
    e['zone_id'] = b.zone_id[e.building_id].values
    e['county_id'] = b.county_id[e.building_id].values
    e['sector_id_six'] = 1*(e.sector_id==61) + 2*(e.sector_id==71) + 3*np.in1d(e.sector_id,[11,21,22,23,31,32,33,42,48,49]) + 4*np.in1d(e.sector_id,[7221,7222,7224]) + 5*np.in1d(e.sector_id,[44,45,7211,7212,7213,7223]) + 6*np.in1d(e.sector_id,[51,52,53,54,55,56,62,81,92])
    e['sector_id_retail_agg'] = e.sector_id*np.logical_not(np.in1d(e.sector_id,[7211,7212,7213])) + 7211*np.in1d(e.sector_id,[7211,7212,7213])
    e['nonres_sqft'] = b.non_residential_sqft[e.building_id].values

    #ZONE VARIABLES
    z = dset.fetch('zones')
    z['zonal_hh'] = hh.groupby('zone_id').size()
    z['zonal_emp'] = e.groupby('zone_id').employees.sum()
    z['zonal_pop'] = hh.groupby('zone_id').persons.sum()
    z['residential_units_zone'] = b.groupby('zone_id').residential_units.sum()
    z['ln_residential_units_zone'] = b.groupby('zone_id').residential_units.sum().apply(np.log1p)
    z['ln_residential_unit_density_zone'] = (b.groupby('zone_id').residential_units.sum()/z.acreage).apply(np.log1p)
    z['non_residential_sqft_zone'] = b.groupby('zone_id').non_residential_sqft.sum()
    z['ln_non_residential_sqft_zone'] = b.groupby('zone_id').non_residential_sqft.sum().apply(np.log1p)
    z['percent_sf'] = b[b.btype_hlcm==3].groupby('zone_id').residential_units.sum()*100.0/(b.groupby('zone_id').residential_units.sum())
    z['avg_unit_price_zone'] = b[(b.residential_units>0)*(b.improvement_value>0)].groupby('zone_id').unit_price_residential.mean()
    z['ln_avg_unit_price_zone'] = b[(b.residential_units>0)*(b.improvement_value>0)].groupby('zone_id').unit_price_residential.mean().apply(np.log1p)
    z['ln_avg_nonres_unit_price_zone'] = b[(b.non_residential_sqft>0)*(b.improvement_value>0)].groupby('zone_id').unit_price_non_residential.mean().apply(np.log1p)
    z['median_age_of_head'] = hh.groupby('zone_id').age_of_head.median()
    z['mean_income'] = hh.groupby('zone_id').income.mean()
    z['median_year_built'] = b.groupby('zone_id').year_built.median().astype('int32')
    z['ln_avg_land_value_per_sqft_zone'] = p.groupby('zone_id').land_value_per_sqft.mean().apply(np.log1p)
    z['median_yearbuilt_post_1990'] = (b.groupby('zone_id').year_built.median()>1990).astype('int32')
    z['median_yearbuilt_pre_1950'] = (b.groupby('zone_id').year_built.median()<1950).astype('int32')
    z['percent_hh_with_child'] = hh[hh.children>0].groupby('zone_id').size()*100.0/z.zonal_hh
    z['percent_renter_hh_in_zone'] = hh[hh.tenure==2].groupby('zone_id').size()*100.0/z.zonal_hh
    z['percent_younghead'] = hh[hh.age_of_head<30].groupby('zone_id').size()*100.0/z.zonal_hh
    z['average_resunit_size'] = b.groupby('zone_id').sqft_per_unit.mean()
    z['zone_contains_park'] = (p[p.lu_type_id==14].groupby('zone_id').size()>0).astype('int32')
    z['emp_sector_agg'] = e[e.sector_id==1].groupby('zone_id').employees.sum()
    z['emp_sector1'] = e[e.sector_id_six==1].groupby('zone_id').employees.sum()
    z['emp_sector2'] = e[e.sector_id_six==2].groupby('zone_id').employees.sum()
    z['emp_sector3'] = e[e.sector_id_six==3].groupby('zone_id').employees.sum()
    z['emp_sector4'] = e[e.sector_id_six==4].groupby('zone_id').employees.sum()
    z['emp_sector5'] = e[e.sector_id_six==5].groupby('zone_id').employees.sum()
    z['emp_sector6'] = e[e.sector_id_six==6].groupby('zone_id').employees.sum()
    z['jobs_within_45min'] = dset.compute_range(z.zonal_emp,45.0)/10000.0
    z['ln_jobs_within_45min'] = dset.compute_range(z.zonal_emp,45.0).apply(np.log1p)
    z['jobs_within_30min'] = dset.compute_range(z.zonal_emp,30.0)/10000.0
    z['ln_jobs_within_30min'] = dset.compute_range(z.zonal_emp,30.0).apply(np.log1p)
    z['jobs_within_20min'] = dset.compute_range(z.zonal_emp,20.0)/10000.0
    z['ln_jobs_within_20min'] = dset.compute_range(z.zonal_emp,20.0).apply(np.log1p)
    z['ln_pop_within_20min'] = dset.compute_range(z.zonal_pop,20.0).apply(np.log1p)
    z['ln_emp_aggsector_within_5min'] = dset.compute_range(z.emp_sector_agg,5.0).apply(np.log1p)
    z['ln_emp_sector1_within_15min'] = dset.compute_range(z.emp_sector1,15.0).apply(np.log1p)
    z['ln_emp_sector2_within_15min'] = dset.compute_range(z.emp_sector2,15.0).apply(np.log1p)
    z['ln_emp_sector3_within_10min'] = dset.compute_range(z.emp_sector3,15.0).apply(np.log1p)
    z['ln_emp_sector3_within_15min'] = dset.compute_range(z.emp_sector3,15.0).apply(np.log1p)
    z['ln_emp_sector3_within_20min'] = dset.compute_range(z.emp_sector3,20.0).apply(np.log1p)
    z['ln_emp_sector4_within_15min'] = dset.compute_range(z.emp_sector4,15.0).apply(np.log1p)
    z['ln_emp_sector5_within_15min'] = dset.compute_range(z.emp_sector5,15.0).apply(np.log1p)
    z['ln_emp_sector6_within_15min'] = dset.compute_range(z.emp_sector6,15.0).apply(np.log1p)
    z['allpurpose_agglosum_floor'] = (z.allpurpose_agglosum>=0)*(z.allpurpose_agglosum)
    
    ##JOINS
    #merge parcels with zones
    pz = pd.merge(p.reset_index(),z,left_on='zone_id',right_index=True)
    pz = pz.set_index('parcel_id')
    #merge buildings with parcels/zones
    del b['county_id']
    del b['zone_id']
    bpz = pd.merge(b,pz,left_on='parcel_id',right_index=True)
    bpz['residential_units_capacity'] = bpz.parcel_sqft/1500 - bpz.residential_units
    bpz.residential_units_capacity[bpz.residential_units_capacity<0] = 0
    dset.d['buildings'] = bpz
    if dset.parcels.index.name != 'parcel_id':
        dset.d['parcels'] = p