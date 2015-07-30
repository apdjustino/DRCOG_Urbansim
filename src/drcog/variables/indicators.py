import pandas as pd, numpy as np, time, os
from sqlalchemy import *

def run(dset, indicator_output_directory, forecast_year):

    engine = create_engine('postgresql://postgres:postgres@localhost:5432/postgres', echo=False)
    z_index = dset.zones.index
    zone_index = pd.Series(index=z_index).fillna(0)

    #Record base values for temporal comparison
    hh = dset.store.households
    e = dset.store.establishments
    b = dset.store.buildings
    p = dset.store.parcels.set_index('parcel_id')
    z = dset.zones
    b['zone_id'] = p.zone_id[b.parcel_id].values
    b['unit_price_res_sqft']=b['unit_price_residential']/b['bldg_sq_ft']
    hh['zone_id'] = b.zone_id[hh.building_id].values
    e['zone_id'] = b.zone_id[e.building_id].values
    b['county_id'] = p.county_id[b.parcel_id].values
    hh['county_id'] = b.county_id[hh.building_id].values
    e['county_id'] = b.county_id[e.building_id].values
    e['sector_id_six'] = 1*(e.sector_id==61) + 2*(e.sector_id==71) + 3*np.in1d(e.sector_id,[11,21,22,23,31,32,33,42,48,49]) + 4*np.in1d(e.sector_id,[7221,7222,7224]) + 5*np.in1d(e.sector_id,[44,45,7211,7212,7213,7223]) + 6*np.in1d(e.sector_id,[51,52,53,54,55,56,62,81,92])
    #Base year county indicators
    county_name = z.groupby('county').size().index.values.tolist()
    base_hh_county = hh.groupby('county_id').size()
    base_pop_county = hh.groupby('county_id').persons.sum()
    base_medinc_county = hh.groupby('county_id').income.median()
    base_emp_county = e.groupby('county_id').employees.sum()
    base_ru_county = b.groupby('county_id').residential_units.sum()
    base_nr_county = b.groupby('county_id').non_residential_sqft.sum()
    base_emp1_county = e[e.sector_id_six==1].groupby('county_id').employees.sum()
    base_emp2_county = e[e.sector_id_six==2].groupby('county_id').employees.sum()
    base_emp3_county = e[e.sector_id_six==3].groupby('county_id').employees.sum()
    base_emp4_county = e[e.sector_id_six==4].groupby('county_id').employees.sum()
    base_emp5_county = e[e.sector_id_six==5].groupby('county_id').employees.sum()
    base_emp6_county = e[e.sector_id_six==6].groupby('county_id').employees.sum()
    base_res_price_county = b[b['bldg_sq_ft']>0].groupby('county_id').unit_price_residential.mean()
    base_non_res_price_county = b[(np.in1d(b.building_type_id,[5,9,17,18,22]))].groupby('county_id').unit_price_non_residential.mean()
    base_year_building_count = b.groupby('county_id').size()

    ##Base year zonal indicators
    base_hh_zone = hh.groupby('zone_id').size()+zone_index
    base_pop_zone = hh.groupby('zone_id').persons.sum()+zone_index
    base_medinc_zone = hh.groupby('zone_id').income.median()+zone_index
    base_ru_zone = b.groupby('zone_id').residential_units.sum()+zone_index
    base_emp_zone = e.groupby('zone_id').employees.sum()+zone_index
    base_emp1_zone = e[e.sector_id_six==1].groupby('zone_id').employees.sum()+zone_index
    base_emp2_zone = e[e.sector_id_six==2].groupby('zone_id').employees.sum()+zone_index
    base_emp3_zone = e[e.sector_id_six==3].groupby('zone_id').employees.sum()+zone_index
    base_emp4_zone = e[e.sector_id_six==4].groupby('zone_id').employees.sum()+zone_index
    base_emp5_zone = e[e.sector_id_six==5].groupby('zone_id').employees.sum()+zone_index
    base_emp6_zone = e[e.sector_id_six==6].groupby('zone_id').employees.sum()+zone_index
    base_nr_zone = b.groupby('zone_id').non_residential_sqft.sum()+zone_index

    base_hh_zone = base_hh_zone.fillna(0)
    base_pop_zone = base_pop_zone.fillna(0)
    base_medinc_zone = base_medinc_zone.fillna(0)
    base_ru_zone = base_ru_zone.fillna(0)
    base_emp_zone = base_emp_zone.fillna(0)
    base_emp1_zone = base_emp1_zone.fillna(0)
    base_emp2_zone = base_emp2_zone.fillna(0)
    base_emp3_zone = base_emp3_zone.fillna(0)
    base_emp4_zone = base_emp4_zone.fillna(0)
    base_emp5_zone = base_emp5_zone.fillna(0)
    base_emp6_zone = base_emp6_zone.fillna(0)
    base_nr_zone = base_nr_zone.fillna(0)
    base_res_price_zone = b.groupby('zone_id').unit_price_residential.mean()
    base_non_res_price_zone = b[(np.isfinite(b.unit_price_non_residential))*(np.in1d(b.building_type_id,[5,9,17,18,22]))*(b.non_residential_sqft>0)].groupby('zone_id').unit_price_non_residential.mean()
    base_zone_building_count = b.groupby('zone_id').size()


    ##Forecast year indicators
    b = dset.buildings

    e = dset.fetch('establishments')
    hh = dset.fetch('households')
    p = dset.fetch('parcels')
    if p.index.name != 'parcel_id':
        p = p.set_index(p['parcel_id'])


    #b['zone_id'] = p.zone_id[b.parcel_id].values
    #hh['zone_id'] = b.zone_id[hh.building_id].values
    #e['zone_id'] = b.zone_id[e.building_id].values
    county_table = pd.read_csv('C:/urbansim/data/taz_county_table.csv', index_col=1)

    hh.drop('zone_id', axis=1, inplace=True)
    #e.drop('zone_id', axis=1, inplace=True)

    hh = pd.merge(hh, b, left_on='building_id', right_index=True)
    #e = pd.merge(e, b, left_on='building_id', right_index=True)

    b['county_id'] = county_table.loc[b.zone_id, "county_id"].values
    hh['county_id'] = county_table.loc[hh.zone_id, "county_id"].values
    e['county_id'] = county_table.loc[e.zone_id, "county_id"].values

    sim_hh_county = hh.groupby('county_id').size()
    sim_pop_county = hh.groupby('county_id').persons.sum()
    sim_medinc_county = hh.groupby('county_id').income.median()
    sim_emp_county = e.groupby('county_id').employees.sum()
    sim_ru_county = b.groupby('county_id').residential_units.sum()
    sim_nr_county = b.groupby('county_id').non_residential_sqft.sum()
    sim_emp1_county = e[e.sector_id_six==1].groupby('county_id').employees.sum()
    sim_emp2_county = e[e.sector_id_six==2].groupby('county_id').employees.sum()
    sim_emp3_county = e[e.sector_id_six==3].groupby('county_id').employees.sum()
    sim_emp4_county = e[e.sector_id_six==4].groupby('county_id').employees.sum()
    sim_emp5_county = e[e.sector_id_six==5].groupby('county_id').employees.sum()
    sim_emp6_county = e[e.sector_id_six==6].groupby('county_id').employees.sum()
    sim_hh_zone = hh.groupby('zone_id').size()
    sim_pop_zone = hh.groupby('zone_id').persons.sum()
    sim_medinc_zone = hh.groupby('zone_id').income.median()
    sim_ru_zone = b.groupby('zone_id').residential_units.sum()
    sim_emp_zone = e.groupby('zone_id').employees.sum()
    sim_emp1_zone = e[e.sector_id_six==1].groupby('zone_id').employees.sum()
    sim_emp2_zone = e[e.sector_id_six==2].groupby('zone_id').employees.sum()
    sim_emp3_zone = e[e.sector_id_six==3].groupby('zone_id').employees.sum()
    sim_emp4_zone = e[e.sector_id_six==4].groupby('zone_id').employees.sum()
    sim_emp5_zone = e[e.sector_id_six==5].groupby('zone_id').employees.sum()
    sim_emp6_zone = e[e.sector_id_six==6].groupby('zone_id').employees.sum()




    sim_nr_zone = b.groupby('zone_id').non_residential_sqft.sum()
    hh_diff_county = sim_hh_county - base_hh_county
    pop_diff_county = sim_pop_county - base_pop_county
    emp_diff_county = sim_emp_county - base_emp_county
    ru_diff_county = sim_ru_county - base_ru_county
    nr_diff_county = sim_nr_county - base_nr_county
    diff_emp1_county = sim_emp1_county - base_emp1_county
    diff_emp2_county = sim_emp2_county - base_emp2_county
    diff_emp3_county = sim_emp3_county - base_emp3_county
    diff_emp4_county = sim_emp4_county - base_emp4_county
    diff_emp5_county = sim_emp5_county - base_emp5_county
    diff_emp6_county = sim_emp6_county - base_emp6_county
    diff_hh_zone = sim_hh_zone - base_hh_zone
    diff_pop_zone = sim_pop_zone - base_pop_zone
    diff_ru_zone = sim_ru_zone - base_ru_zone
    diff_emp_zone = sim_emp_zone - base_emp_zone
    diff_emp1_zone = sim_emp1_zone - base_emp1_zone
    diff_emp2_zone = sim_emp2_zone - base_emp2_zone
    diff_emp3_zone = sim_emp3_zone - base_emp3_zone
    diff_emp4_zone = sim_emp4_zone - base_emp4_zone
    diff_emp5_zone = sim_emp5_zone - base_emp5_zone
    diff_emp6_zone = sim_emp6_zone - base_emp6_zone
    diff_nr_zone = sim_nr_zone - base_nr_zone


    sim_res_price_county = b[(np.in1d(b.building_type_id,[2,3,20,24]))*(b['bldg_sq_ft2']>0)].groupby('county_id').unit_price_residential.mean()
    sim_non_res_price_county = b[(np.isfinite(b.unit_price_non_residential))*(np.in1d(b.building_type_id,[5,9,17,18,22]))*(b.non_residential_sqft>0)].groupby('county_id').unit_price_non_residential.mean()
    sim_non_res_price_office=b[(np.isfinite(b.unit_price_non_residential))*(np.in1d(b.building_type_id,[5]))*(b.non_residential_sqft>0)].groupby('county_id').unit_price_non_residential.mean()

    sim_res_price_zone = b.groupby('zone_id').unit_price_residential.mean()
    sim_non_res_price_zone = b.groupby('zone_id').unit_price_non_residential.mean()
    diff_res_price_county = sim_res_price_county - base_res_price_county
    diff_non_res_price_county = sim_non_res_price_county - base_non_res_price_county
    diff_res_price_zone = sim_res_price_zone - base_res_price_zone
    diff_non_res_price_zone = sim_non_res_price_zone - base_non_res_price_zone
    sim_density_county = sim_hh_county / (b.groupby('county_id').bldg_sq_ft.sum())
    sim_density_zone = sim_hh_zone / (b.groupby('zone_id').bldg_sq_ft.sum())


    building_count_county = b.groupby('county_id').size()
    building_count_zone = b.groupby('zone_id').size()
    ######
    z = dset.fetch('zones')
    zsumm = pd.DataFrame(index=z.index)
    zsumm['sim_year'] = forecast_year
    zsumm['hh_base'] = base_hh_zone
    zsumm['pop_base'] = base_pop_zone
    zsumm['median_income_base'] = base_medinc_zone
    zsumm['ru_base'] = base_ru_zone
    zsumm['emp_base'] = base_emp_zone
    zsumm['emp1_base'] = base_emp1_zone
    zsumm['emp2_base'] = base_emp2_zone
    zsumm['emp3_base'] = base_emp3_zone
    zsumm['emp4_base'] = base_emp4_zone
    zsumm['emp5_base'] = base_emp5_zone
    zsumm['emp6_base'] = base_emp6_zone
    zsumm['nr_base'] = base_nr_zone
    zsumm['hh_sim'] = sim_hh_zone
    zsumm['pop_sim'] = sim_pop_zone
    zsumm['median_income_sim'] = sim_medinc_zone
    zsumm['ru_sim'] = sim_ru_zone
    zsumm['emp_sim'] = sim_emp_zone
    zsumm['emp1_sim'] = sim_emp1_zone
    zsumm['emp2_sim'] = sim_emp2_zone
    zsumm['emp3_sim'] = sim_emp3_zone
    zsumm['emp4_sim'] = sim_emp4_zone
    zsumm['emp5_sim'] = sim_emp5_zone
    zsumm['emp6_sim'] = sim_emp6_zone
    zsumm['nr_sim'] = sim_nr_zone
    zsumm['hh_diff'] = diff_hh_zone
    zsumm['pop_diff'] = diff_pop_zone
    zsumm['ru_diff'] = diff_ru_zone
    zsumm['emp_diff'] = diff_emp_zone
    zsumm['emp1_diff'] = diff_emp1_zone
    zsumm['emp2_diff'] = diff_emp2_zone
    zsumm['emp3_diff'] = diff_emp3_zone
    zsumm['emp4_diff'] = diff_emp4_zone
    zsumm['emp5_diff'] = diff_emp5_zone
    zsumm['emp6_diff'] = diff_emp6_zone
    zsumm['nr_diff'] = diff_nr_zone
    zsumm['res_price_base'] = base_res_price_zone
    zsumm['non_res_price_base'] = base_non_res_price_zone
    zsumm['res_price_sim'] = sim_res_price_zone
    zsumm['non_res_price_sim'] = sim_non_res_price_zone
    zsumm['res_price_diff'] = diff_res_price_zone
    zsumm['non_res_price_diff'] = diff_non_res_price_zone

    zsumm['sim_density_zone'] = sim_density_zone

    zsumm['building_count'] = building_count_zone
    zsumm['base_year_building_count']= base_zone_building_count

    ######
    csumm = pd.DataFrame(index=sim_hh_county.index)
    csumm['county_name']=county_name
    csumm['sim_year'] = forecast_year
    csumm['hh_base'] = base_hh_county
    csumm['pop_base'] = base_pop_county
    csumm['median_income_base'] = base_medinc_county
    csumm['ru_base'] = base_ru_county
    csumm['emp_base'] = base_emp_county
    csumm['emp1_base'] = base_emp1_county
    csumm['emp2_base'] = base_emp2_county
    csumm['emp3_base'] = base_emp3_county
    csumm['emp4_base'] = base_emp4_county
    csumm['emp5_base'] = base_emp5_county
    csumm['emp6_base'] = base_emp6_county
    csumm['nr_base'] = base_nr_county
    csumm['hh_sim'] = sim_hh_county
    csumm['pop_sim'] = sim_pop_county
    csumm['median_income_sim'] = sim_medinc_county
    csumm['ru_sim'] = sim_ru_county
    csumm['emp_sim'] = sim_emp_county
    csumm['emp1_sim'] = sim_emp1_county
    csumm['emp2_sim'] = sim_emp2_county
    csumm['emp3_sim'] = sim_emp3_county
    csumm['emp4_sim'] = sim_emp4_county
    csumm['emp5_sim'] = sim_emp5_county
    csumm['emp6_sim'] = sim_emp6_county
    csumm['nr_sim'] = sim_nr_county
    csumm['hh_diff'] = hh_diff_county
    csumm['pop_diff'] = pop_diff_county
    csumm['ru_diff'] = ru_diff_county
    csumm['emp_diff'] = emp_diff_county
    csumm['emp1_diff'] = diff_emp1_county
    csumm['emp2_diff'] = diff_emp2_county
    csumm['emp3_diff'] = diff_emp3_county
    csumm['emp4_diff'] = diff_emp4_county
    csumm['emp5_diff'] = diff_emp5_county
    csumm['emp6_diff'] = diff_emp6_county
    csumm['nr_diff'] = nr_diff_county
    csumm['res_price_base'] = base_res_price_county
    csumm['non_res_price_base'] = base_non_res_price_county
    csumm['res_price_sim'] = sim_res_price_county
    csumm['non_res_price_sim'] = sim_non_res_price_county
    csumm['non_res_price_office_sim'] = sim_non_res_price_office
    csumm['res_price_diff'] = diff_res_price_county
    csumm['non_res_price_diff'] = diff_non_res_price_county

    csumm['sim_density_county'] = sim_density_county

    csumm['building_count'] = building_count_county
    csumm['base_building_count']=base_year_building_count

    #csumm['ln_pop_within_20min_sim']=sim_pop_20min

    ######
    #csumm.to_csv(os.path.join(indicator_output_directory,'county_summary%s_%s.csv' % (forecast_year,time.strftime('%c').replace('/','').replace(':','').replace(' ',''))))
    #zsumm.to_csv(os.path.join(indicator_output_directory,'zone_summary%s_%s.csv' % (forecast_year,time.strftime('%c').replace('/','').replace(':','').replace(' ',''))))

    csumm.to_sql('county_summary', engine, if_exists='append')
    zsumm.to_sql('zone_summary', engine, if_exists='append')