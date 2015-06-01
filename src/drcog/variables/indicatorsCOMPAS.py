import pandas as pd, numpy as np, time, os

def run(dset, indicator_output_directory, forecast_year):

    z_index = dset.zones.index
    zone_index = pd.Series(index=z_index).fillna(0)
    
    #Record base values for temporal comparison
    hh = dset.store.households
    e = dset.store.establishments
    b = dset.store.buildings
    p = dset.store.parcels.set_index('parcel_id')
    b['zone_id'] = p.zone_id[b.parcel_id].values
    hh['zone_id'] = b.zone_id[hh.building_id].values
    e['zone_id'] = b.zone_id[e.building_id].values
    b['county_id'] = p.county_id[b.parcel_id].values
    hh['county_id'] = b.county_id[hh.building_id].values
    e['county_id'] = b.county_id[e.building_id].values


     ##Calculate the income breakpoints
    low_income_breakpoint = hh.income.quantile(.11)
    high_income_breakpoint = hh.income.quantile(.75)

    ##Tag households according to income bucket
    hh['low_inc_hh'] = hh.income<low_income_breakpoint
    hh['med_inc_hh'] = (hh.income>=low_income_breakpoint)*(hh.income<high_income_breakpoint)
    hh['high_inc_hh'] = hh.income>=high_income_breakpoint

    ##Zonal household variables
    hh_pop = hh.groupby('zone_id').persons.sum()
    low_inc_hh = hh[hh.low_inc_hh==True].groupby('zone_id').size()
    med_inc_hh = hh[hh.med_inc_hh==True].groupby('zone_id').size()
    high_inc_hh = hh[hh.high_inc_hh==True].groupby('zone_id').size()


    # Employment (without home-based-employment)
    prod_sectors = [11,21,22,23,31,32,33,42]
    retail_sectors = [44,45,7211,7212,7213,7223,7221,7222,7224]
    service_sectors = [48,49,51,52,53,54,55,56,62,81,92, 61, 71]

    total_employment = e[e.home_based_status==0].groupby('zone_id').employees.sum()
    prod_emp = e[(np.in1d(e.sector_id,prod_sectors))*(e.home_based_status==0)].groupby('zone_id').employees.sum()
    retail_emp = e[(np.in1d(e.sector_id,retail_sectors))*(e.home_based_status==0)].groupby('zone_id').employees.sum()
    service_emp = e[(np.in1d(e.sector_id,service_sectors))*(e.home_based_status==0)].groupby('zone_id').employees.sum()


    z = dset.fetch('zones')
    zsummb = pd.DataFrame(index=z.index)
    zsummb['HH_Pop'] = hh_pop
    zsummb['Low_Inc_HH'] = low_inc_hh
    zsummb['Med_Inc_HH'] = med_inc_hh
    zsummb['High_Inc_HH'] = high_inc_hh
    zsummb['Prod_Emp'] = prod_emp
    zsummb['Retail_Emp'] = retail_emp
    zsummb['Service_Emp'] = service_emp
    zsummb['TotalEmployment'] = total_employment

    taz=pd.read_csv('C:\urbansim\data\TAZ.csv')
    taz.columns=['TAZID','zone_id']
    zsummb=pd.merge(taz, zsummb, left_on='zone_id', right_index=True)
    zsummb.index=zsummb['TAZID']
    del zsummb['TAZID']


    ##Forecast year indicators
    b = dset.fetch('buildings')
    e = dset.fetch('establishments')
    hh = dset.fetch('households')
    p = dset.fetch('parcels')
    b['county_id'] = p.county_id[b.parcel_id].values
    hh['county_id'] = b.county_id[hh.building_id].values
    e['county_id'] = b.county_id[e.building_id].values
    b['zone_id'] = p.zone_id[b.parcel_id].values
    hh['zone_id'] = b.zone_id[hh.building_id].values
    e['zone_id'] = b.zone_id[e.building_id].values



     ##Calculate the income breakpoints
    low_income_breakpoint = hh.income.quantile(.11)
    high_income_breakpoint = hh.income.quantile(.75)

    ##Tag households according to income bucket
    hh['low_inc_hh'] = hh.income<low_income_breakpoint
    hh['med_inc_hh'] = (hh.income>=low_income_breakpoint)*(hh.income<high_income_breakpoint)
    hh['high_inc_hh'] = hh.income>=high_income_breakpoint

    ##Zonal household variables
    sim_hh_pop = hh.groupby('zone_id').persons.sum()
    sim_low_inc_hh = hh[hh.low_inc_hh==True].groupby('zone_id').size()
    sim_med_inc_hh = hh[hh.med_inc_hh==True].groupby('zone_id').size()
    sim_high_inc_hh = hh[hh.high_inc_hh==True].groupby('zone_id').size()


    # Employment (without home-based-employment)
    sim_total_employment = e[e.home_based_status==0].groupby('zone_id').employees.sum()
    prod_sectors = [11,21,22,23,31,32,33,42]
    retail_sectors = [44,45,7211,7212,7213,7223,7221,7222,7224]
    service_sectors = [48,49,51,52,53,54,55,56,62,81,92, 61, 71]

    sim_prod_emp = e[(np.in1d(e.sector_id,prod_sectors))*(e.home_based_status==0)].groupby('zone_id').employees.sum()
    sim_retail_emp = e[(np.in1d(e.sector_id,retail_sectors))*(e.home_based_status==0)].groupby('zone_id').employees.sum()
    sim_service_emp = e[(np.in1d(e.sector_id,service_sectors))*(e.home_based_status==0)].groupby('zone_id').employees.sum()

    ######
    z = dset.fetch('zones')
    zsumm = pd.DataFrame(index=z.index)
    zsumm['HH_Pop'] = sim_hh_pop
    zsumm['Low_Inc_HH'] = sim_low_inc_hh
    zsumm['Med_Inc_HH'] = sim_med_inc_hh
    zsumm['High_Inc_HH'] =sim_high_inc_hh
    zsumm['Prod_Emp'] = sim_prod_emp
    zsumm['Retail_Emp'] = sim_retail_emp
    zsumm['Service_Emp'] = sim_service_emp
    zsumm['TotalEmployment'] = sim_total_employment

    ###### TAZ
    taz=pd.read_csv('C:\urbansim\data\TAZ.csv')
    taz.columns=['TAZID','zone_id']
    zsumm=pd.merge(taz, zsumm, left_on='zone_id', right_index=True)
    zsumm.index=zsumm['TAZID']
    del zsumm['TAZID']

    if forecast_year==2011:
        zsumm.to_csv(os.path.join(indicator_output_directory,'zone_summary_COMPAS%s_%s.csv' % (forecast_year,time.strftime('%c').replace('/','').replace(':','').replace(' ',''))))
        zsummb.to_csv(os.path.join(indicator_output_directory,'zone_summary_COMPAS%s_%s.csv' % (2010,time.strftime('%c').replace('/','').replace(':','').replace(' ',''))))
    else:
        zsumm.to_csv(os.path.join(indicator_output_directory,'zone_summary_COMPAS%s_%s.csv' % (forecast_year,time.strftime('%c').replace('/','').replace(':','').replace(' ',''))))