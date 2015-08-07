__author__ = 'jmartinez'
import pandas as pd, numpy as np, os
from synthicity.utils import misc
elcm_configuration = {'building_sqft_per_job_table':'building_sqft_per_job','scaling_factor':1.0}
import multiprocessing as mp


def compute_range(travel_data, attr, dist, agg=np.sum):
    t_data = travel_data[travel_data.am_single_vehicle_to_work_travel_time<dist]
    t_data["attr"] = attr[t_data.to_zone_id].values
    return t_data.groupby(level=0).attr.apply(agg)
    

def get_tables(dset):
    p = dset.parcels
    del p['county_id']
    zone_county=pd.read_csv('C:\urbansim\data/TAZ_County_Table.csv')
    zone_county=zone_county.set_index('zone_id')
    zone_county=zone_county[['county_id']]
    p=pd.merge(p,zone_county, left_on='zone_id', right_index=True)

    b = dset.fetch('buildings',building_sqft_per_job_table=elcm_configuration['building_sqft_per_job_table'],bsqft_job_scaling=elcm_configuration['scaling_factor'])
    b.loc[:, 'zone_id'] = p.zone_id[b.parcel_id].values
    hh_estim = dset.fetch('households_for_estimation')
    #hh = dset.fetch('households')
    hh = dset.households
    e = dset.establishments
    z = dset.zones
    #e = dset.fetch('establishments')
    hh['zone_id'] = hh.zone_id[hh.building_id].values
    e['zone_id'] = b.zone_id[e.building_id].values
    #z = dset.fetch('zones')
    travel_data = dset.fetch('travel_data').reset_index(level=1)
    bsqft_job = dset.building_sqft_per_job
    return p, b, hh_estim, hh, e, z, travel_data, bsqft_job

def process_parcels(p, q):
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

    q.put(p)

def process_buildings(p, b, bsqft_job, e, q):
    b = b[['building_type_id','improvement_value','land_area','non_residential_sqft','parcel_id','residential_units','sqft_per_unit','stories','tax_exempt','year_built','bldg_sq_ft','unit_price_non_residential','unit_price_residential']]
    b.loc[:, 'zone_id'] = p.zone_id[b.parcel_id].values


    #bsqft_job.building_sqft_per_job = bsqft_job.building_sqft_per_job
    b = pd.merge(b,bsqft_job,left_on=['zone_id','building_type_id'],right_index=True,how='left')
    b["non_residential_units"] = b.non_residential_sqft/b.building_sqft_per_job#####
    b["base_year_jobs"] = e.groupby('building_id').employees.sum()
    # things get all screwed up if you have overfull buildings
    b["non_residential_units"] = b[["non_residential_units","base_year_jobs"]].max(axis=1)
    b["all_units"] = b.residential_units + b.non_residential_units


    b['county_id'] = p.county_id[b.parcel_id].values
    b['townhome'] = (b.building_type_id==24).astype('int32')
    b['multifamily'] = (np.in1d(b.building_type_id,[2,3])).astype('int32')
    b['office'] = (b.building_type_id==5).astype('int32')
    b['retail_or_restaurant'] = (np.in1d(b.building_type_id,[17,18])).astype('int32')
    b['industrial_building'] = (np.in1d(b.building_type_id,[9,22])).astype('int32')
    b['residential_sqft'] = (b.bldg_sq_ft - b.non_residential_sqft)
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
    b[ 'unit_price_res_sqft']=b[b.residential_units>0].unit_price_residential/b[b.residential_units>0].bldg_sq_ft
    #p['nonres_far'] = (b.groupby('parcel_id').non_residential_sqft.sum()/p.acres).apply(np.log1p)
    #p['ln_units_per_acre'] = (b.groupby('parcel_id').residential_units.sum()/p.acres).apply(np.log1p)

    q.put(b)

def process_hh_estabs(hh_estim, hh, e, b, q):
    hh_estim['tenure'] = 1
    hh_estim.loc[hh_estim.own>1, "tenure"] = 2  # corrected chained index error
    hh_estim['income']=0
    hh_estim.loc[hh_estim.income_group==1, "income"] = 7500  # corrected chained index error
    hh_estim.loc[hh_estim.income_group==2, "income"] = 17500  # corrected chained index error
    hh_estim.loc[hh_estim.income_group==3, "income"] = 25000  # corrected chained index error
    hh_estim.loc[hh_estim.income_group==4, "income"] = 35000  # corrected chained index error
    hh_estim.loc[hh_estim.income_group==5, "income"] = 45000  # corrected chained index error
    hh_estim.loc[hh_estim.income_group==6, "income"] = 55000  # corrected chained index error
    hh_estim.loc[hh_estim.income_group==7, "income"] = 67500  # corrected chained index error
    hh_estim.loc[hh_estim.income_group==8, "income"] = 87500  # corrected chained index error
    hh_estim.loc[hh_estim.income_group==9, "income"] = 117500  # corrected chained index error
    hh_estim.loc[hh_estim.income_group==10, "income"] = 142500  # corrected chained index error
    hh_estim.loc[hh_estim.income_group==11, "income"] = 200000  # corrected chained index error

    for table in [hh_estim, hh]:
        choosers = table
        choosers['zone_id'] = b.zone_id[choosers.building_id].values
        choosers['building_type_id'] = b.building_type_id[choosers.building_id].values
        #choosers['county_id'] = b.county_id[choosers.building_id].values
        choosers['btype'] = 1*(choosers.building_type_id==2) + 2*(choosers.building_type_id==3) + 3*(choosers.building_type_id==20) + 4*np.invert(np.in1d(choosers.building_type_id,[2,3,20]))
        choosers['income_3_tenure'] = 1 * (choosers.income < 60000)*(choosers.tenure == 1) + 2 * np.logical_and(choosers.income >= 60000, choosers.income < 120000)*(choosers.tenure == 1) + 3*(choosers.income >= 120000)*(choosers.tenure == 1) + 4*(choosers.income < 40000)*(choosers.tenure == 2) + 5*(choosers.income >= 40000)*(choosers.tenure == 2)
        choosers['younghead'] = choosers.age_of_head<30
        choosers['hh_with_child'] = choosers.children>0
        choosers['ln_income'] = choosers.income.apply(np.log1p)
        choosers['income5xlt'] = choosers.income*5.0
        choosers['income10xlt'] = choosers.income*5.0
        choosers['wkrs_hhs'] = choosers.workers*1.0/choosers.persons

    #e['zone_id'] = b.zone_id[e.building_id].values

    #e['county_id'] = b.county_id[e.building_id].values
    e['sector_id_six'] = 1*(e.sector_id==61) + 2*(e.sector_id==71) + 3*np.in1d(e.sector_id,[11,21,22,23,31,32,33,42,48,49]) + 4*np.in1d(e.sector_id,[7221,7222,7224]) + 5*np.in1d(e.sector_id,[44,45,7211,7212,7213,7223]) + 6*np.in1d(e.sector_id,[51,52,53,54,55,56,62,81,92])
    e['sector_id_retail_agg'] = e.sector_id*np.logical_not(np.in1d(e.sector_id,[7211,7212,7213])) + 7211*np.in1d(e.sector_id,[7211,7212,7213])
    e['nonres_sqft'] = b.non_residential_sqft[e.building_id].values

    q.put([e, hh, hh_estim])

def process_zones(z, hh, e, b, p, travel_data, q):
    del z['county']
    z['zone_id']=z.index
    zone_county=pd.read_csv(os.path.join(misc.data_dir(), 'TAZ_County_Table.csv'))
    zone_county=zone_county.set_index('zone_id')
    zone_county=zone_county[['county']]
    z=pd.merge(z,zone_county, left_on='zone_id', right_index=True)
    del z['zone_id']
    zu=z
    #end of fix


    z['zonal_hh'] = hh.groupby('zone_id').size()
    z['zonal_emp'] = e.groupby('zone_id').employees.sum()

    z['zone_id']=z.index
    
    del z['zone_id']

    #z['residential_sqft_zone'] = b.groupby('zone_id').residential_sqft.sum()
    z['zonal_pop'] = hh.groupby('zone_id').persons.sum()
    z['residential_units_zone'] = b.groupby('zone_id').residential_units.sum()
    z['ln_residential_units_zone'] = b.groupby('zone_id').residential_units.sum().apply(np.log1p)
    z['ln_residential_unit_density_zone'] = (b.groupby('zone_id').residential_units.sum()/z.acreage).apply(np.log1p)
    z['non_residential_sqft_zone'] = b.groupby('zone_id').non_residential_sqft.sum()
    z['ln_non_residential_sqft_zone'] = b.groupby('zone_id').non_residential_sqft.sum().apply(np.log1p)
    #z['percent_sf'] = b[b.btype_hlcm==3].groupby('zone_id').residential_units.sum()*100.0/(b.groupby('zone_id').residential_units.sum())
    z['avg_unit_price_zone'] = b[(b.residential_units>0)*(b.improvement_value>0)].groupby('zone_id').unit_price_residential.mean()
    z['ln_avg_unit_price_zone'] = b[(b.residential_units>0)*(b.improvement_value>0)].groupby('zone_id').unit_price_residential.mean().apply(np.log1p)
    z['ln_avg_nonres_unit_price_zone'] = b[(b.non_residential_sqft>0)*(b.improvement_value>0)].groupby('zone_id').unit_price_non_residential.mean().apply(np.log1p)
    z['median_age_of_head'] = hh.groupby('zone_id').age_of_head.median()
    z['mean_income'] = hh.groupby('zone_id').income.mean()
    z['median_year_built'] = b.groupby('zone_id').year_built.median().astype('int32')
    #z['ln_avg_land_value_per_sqft_zone'] = p.groupby('zone_id').land_value_per_sqft.mean().apply(np.log1p)
    z['median_yearbuilt_post_1990'] = (b.groupby('zone_id').year_built.median()>1990).astype('int32')
    z['median_yearbuilt_pre_1950'] = (b.groupby('zone_id').year_built.median()<1950).astype('int32')
    z['percent_hh_with_child'] = hh[hh.children>0].groupby('zone_id').size()*100.0/z.zonal_hh
    z['percent_renter_hh_in_zone'] = hh[hh.tenure==2].groupby('zone_id').size()*100.0/z.zonal_hh
    z['percent_younghead'] = hh[hh.age_of_head<30].groupby('zone_id').size()*100.0/z.zonal_hh
    z['average_resunit_size'] = b.groupby('zone_id').sqft_per_unit.mean()
    z['zone_contains_park'] = (p[p.lu_type_id==14].groupby('zone_id').size()>0).astype('int32')
    z['emp_sector_agg'] = e[e.sector_id==1].groupby('zone_id').employees.sum()
    #z['emp_sector1'] = e[e.sector_id_six==1].groupby('zone_id').employees.sum()
    #z['emp_sector2'] = e[e.sector_id_six==2].groupby('zone_id').employees.sum()
    #z['emp_sector3'] = e[e.sector_id_six==3].groupby('zone_id').employees.sum()
    #z['emp_sector4'] = e[e.sector_id_six==4].groupby('zone_id').employees.sum()
    #z['emp_sector5'] = e[e.sector_id_six==5].groupby('zone_id').employees.sum()
    #z['emp_sector6'] = e[e.sector_id_six==6].groupby('zone_id').employees.sum()
    z['jobs_within_45min'] = compute_range(travel_data, z.zonal_emp,45.0)
    z['ln_jobs_within_45min'] = compute_range(travel_data, z.zonal_emp,45.0).apply(np.log1p)
    z['jobs_within_30min'] = compute_range(travel_data, z.zonal_emp,30.0)
    z['ln_jobs_within_30min'] = compute_range(travel_data, z.zonal_emp,30.0).apply(np.log1p)
    z['jobs_within_20min'] = compute_range(travel_data, z.zonal_emp,20.0)
    z['jobs_within_15min'] = compute_range(travel_data, z.zonal_emp,15.0)
    z['ln_jobs_within_20min'] = compute_range(travel_data, z.zonal_emp,20.0).apply(np.log1p)
    z['ln_pop_within_20min'] = compute_range(travel_data, z.zonal_pop,20.0).apply(np.log1p)
    z['ln_emp_aggsector_within_5min'] = compute_range(travel_data, z.emp_sector_agg,5.0).apply(np.log1p)
    # z['ln_emp_sector1_within_15min'] = compute_range(travel_data, z.emp_sector1,15.0).apply(np.log1p)
    # z['ln_emp_sector2_within_15min'] = compute_range(travel_data, z.emp_sector2,15.0).apply(np.log1p)
    # z['ln_emp_sector3_within_10min'] = compute_range(travel_data, z.emp_sector3,15.0).apply(np.log1p)
    # z['ln_emp_sector3_within_15min'] = compute_range(travel_data, z.emp_sector3,15.0).apply(np.log1p)
    # z['ln_emp_sector3_within_20min'] = compute_range(travel_data, z.emp_sector3,20.0).apply(np.log1p)
    # z['ln_emp_sector4_within_15min'] = compute_range(travel_data, z.emp_sector4,15.0).apply(np.log1p)
    # z['ln_emp_sector5_within_15min'] = compute_range(travel_data, z.emp_sector5,15.0).apply(np.log1p)
    # z['ln_emp_sector6_within_15min'] = compute_range(travel_data, z.emp_sector6,15.0).apply(np.log1p)
    z['allpurpose_agglosum_floor'] = (z.allpurpose_agglosum>=0)*(z.allpurpose_agglosum)

    q.put(z)




if __name__ =='__main__':
    from drcog.models import dataset
    import multiprocessing as mp
    from functools import partial

    dset = dataset.DRCOGDataset(os.path.join(misc.data_dir(),'drcog.h5'))

    p, b, hh_est, hh, e, z, travel_data, bsqft_job = get_tables(dset)
    print p.columns
    jobs = []
    out_q1 = mp.Queue()
    out_q2 = mp.Queue()
    out_q3 = mp.Queue()
    out_q4 = mp.Queue()

    map_parcel = partial(process_parcels, p)
    map_building = partial(process_buildings, p, b, bsqft_job, e)
    map_hh_e = partial(process_hh_estabs, hh_est, hh, e, b)
    map_zones = partial(process_zones, z, hh ,e, b, p, travel_data)

    p1 = mp.Process(target=map_parcel, args=(out_q1,))
    p2 = mp.Process(target=map_building, args=(out_q2,))
    p3 = mp.Process(target=map_hh_e, args=(out_q3,))
    p4 = mp.Process(target=map_zones, args=(out_q4,))

    #jobs.append(p1)
    p1.start()
    #jobs.append(p2)
    p2.start()
    #jobs.append(p3)
    p3.start()
    #jobs.append(p4)
    p4.start()

    # p1.join()
    # p2.join()
    # p3.join()
    # p4.join()

    #test = out_q.get()
    print test.columns










