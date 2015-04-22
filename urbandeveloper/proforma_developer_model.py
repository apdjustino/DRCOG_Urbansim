import pandas as pd, numpy as np
import sys, time, random, string, os
from synthicity.utils import misc
from urbandeveloper import spotproforma, developer

def run(dset,hh_zone1,emp_zone1,developer_configuration,sim_year, elasticity_res=None, elasticity_non_res=None):
    #Record post-demand-model change in zone-level household/job totals
    hh = dset.fetch('households')
    e = dset.fetch('establishments')
    buildings = dset.fetch('buildings')
    parcels = dset.fetch('parcels')
    buildings['zone_id'] = parcels.zone_id[buildings.parcel_id].values
    e['zone_id'] = buildings.zone_id[e.building_id].values
    hh['zone_id'] = buildings.zone_id[hh.building_id].values
    hh_zone2 = hh.groupby('zone_id').size()
    emp_zone2 = e.groupby('zone_id').employees.sum()
    zdiff = pd.DataFrame(index=dset.zones.index) #######
    zdiff['hh_zone1'] = hh_zone1
    zdiff['hh_zone2'] = hh_zone2
    zdiff['emp_zone1'] = emp_zone1
    zdiff['emp_zone2'] = emp_zone2
    zdiff = zdiff.fillna(0)
    zdiff.hh_zone2 = zdiff.hh_zone2+5
    zdiff.emp_zone2 = zdiff.emp_zone2+5
    hh_zone_diff = (zdiff.hh_zone2 - zdiff.hh_zone1)
    emp_zone_diff = (zdiff.emp_zone2 - zdiff.emp_zone1)

    #####Get the user inputted zone args
    if developer_configuration['zonal_levers']:
        zone_args = pd.read_csv(os.path.join(misc.data_dir(),'devmodal_zone_args.csv')).set_index('zone_id')
        ##Getting county_id into zone_args.  Eventually, lets move the dset.zones operations to the varlib
        dset.zones['county_id'] = 0
        dset.zones.county_id[dset.zones.county=='Adams'] = 8001
        dset.zones.county_id[dset.zones.county=='Arapahoe'] = 8005
        dset.zones.county_id[dset.zones.county=='Boulder'] = 8013
        dset.zones.county_id[dset.zones.county=='Broomfield'] = 8014
        dset.zones.county_id[dset.zones.county=='Clear Creek'] = 8019
        dset.zones.county_id[dset.zones.county=='Denver'] = 8031
        dset.zones.county_id[dset.zones.county=='Douglas'] = 8035
        dset.zones.county_id[dset.zones.county=='Elbert'] = 8039
        dset.zones.county_id[dset.zones.county=='Gilpin'] = 8047
        dset.zones.county_id[dset.zones.county=='Jefferson'] = 8059
        dset.zones.county_id[dset.zones.county=='Weld'] = 8123
        zone_args['cid'] = dset.zones.county_id
        ##Loading/applying county calib factors to scale the zone args
        county_args = pd.read_csv(os.path.join(misc.data_dir(),'county_calib.csv')).set_index('county_id')
        zone_args = pd.merge(zone_args,county_args,left_on='cid',right_index=True)
        zone_args.res_price_factor = zone_args.res_price_factor*zone_args.cres_price_factor
        zone_args.nonres_price_factor = zone_args.nonres_price_factor*zone_args.cnonres_price_factor
        zone_args.cost_factor = zone_args.cost_factor*zone_args.ccost_factor
        emp_zone_diff = emp_zone_diff*zone_args.cemp_demand_factor
        hh_zone_diff = hh_zone_diff*zone_args.chh_demand_factor
    else:
        zone_args = None

    # ##########################################
    # #### Getting possible rents by use here ##
    # ##########################################
    buildings = buildings[['building_type_id','improvement_value','land_area','non_residential_sqft','parcel_id','residential_units','sqft_per_unit','stories','tax_exempt','year_built','bldg_sq_ft','unit_price_non_residential','unit_price_residential','building_sqft_per_job','non_residential_units','base_year_jobs','all_units']]
    buildings['zone_id'] = parcels.zone_id[buildings.parcel_id].values
    res_buildings = buildings[buildings.unit_price_residential>0]
    nonres_buildings = buildings[buildings.unit_price_non_residential>0]
    nonres_buildings_office = nonres_buildings[nonres_buildings.building_type_id==5]
    nonres_buildings_retail = nonres_buildings[np.in1d(nonres_buildings.building_type_id,[17,18])]
    nonres_buildings_industrial = nonres_buildings[np.in1d(nonres_buildings.building_type_id,[9,22])]
    res_buildings['resprice_sqft'] = res_buildings.unit_price_residential/res_buildings.sqft_per_unit
    zonal_resprice_sqft = res_buildings.groupby('zone_id').resprice_sqft.mean()
    zonal_nonresprice_office = nonres_buildings_office.groupby('zone_id').unit_price_non_residential.mean()
    zonal_nonresprice_retail = nonres_buildings_retail.groupby('zone_id').unit_price_non_residential.mean()
    zonal_nonresprice_industrial = nonres_buildings_industrial.groupby('zone_id').unit_price_non_residential.mean()
    zonal_resrent = zonal_resprice_sqft/17.9  
    zonal_nonresrent_office = zonal_nonresprice_office/17.9
    zonal_nonresrent_retail = zonal_nonresprice_retail/17.9
    zonal_nonresrent_industrial = zonal_nonresprice_industrial/17.9
    if zone_args is not None:  #####Make sure no nulls in the prices either...
        zonal_resrent = zonal_resrent * zone_args.res_price_factor
        zonal_nonresrent_office = zonal_nonresprice_office * zone_args.nonres_price_factor
        zonal_nonresrent_retail = zonal_nonresprice_retail * zone_args.nonres_price_factor
        zonal_nonresrent_industrial = zonal_nonresprice_industrial * zone_args.nonres_price_factor
        zonal_avg_rents = pd.DataFrame({'resrent':zonal_resrent,'nonresrent_office':zonal_nonresrent_office,'nonresrent_retail':zonal_nonresrent_retail,'nonresrent_industrial':zonal_nonresrent_industrial,'cost_factor':zone_args.cost_factor,'allowable_density_factor':zone_args.allowable_density_factor})
    else:
        zonal_avg_rents = pd.DataFrame({'resrent':zonal_resrent,'nonresrent_office':zonal_nonresrent_office,'nonresrent_retail':zonal_nonresrent_retail,'nonresrent_industrial':zonal_nonresrent_industrial})
    avgrents = pd.merge(parcels,zonal_avg_rents,left_on='zone_id',right_index=True,how='left')
    avgrents['residential'] = avgrents.resrent
    avgrents['office'] = avgrents.nonresrent_office
    avgrents['retail'] = avgrents.nonresrent_retail
    avgrents['industrial'] = avgrents.nonresrent_industrial
    if zone_args is not None:
        avgrents = avgrents[['residential','office','retail','industrial','cost_factor','allowable_density_factor']]
    else:
        avgrents = avgrents[['residential','office','retail','industrial']]
    avgrents = avgrents.fillna(.1)
    #avgrents.residential[np.isinf(avgrents.residential)] = .2
    avgrents.residential[avgrents.residential<.2] = .2
    avgrents.office[avgrents.office<1] = 1
    avgrents.retail[avgrents.retail<1] = 1
    avgrents.industrial[avgrents.industrial<1] = 1

    ####################GET PARCEL LEVEL ATTRIBUTES
    buildings['bldg_sq_ft'] = buildings.non_residential_sqft + buildings.residential_units*buildings.sqft_per_unit
    buildings['impval'] = buildings.non_residential_sqft*buildings.unit_price_non_residential + buildings.residential_units*buildings.unit_price_residential
    far_predictions = pd.DataFrame(index=parcels.index)
    far_predictions['current_yearly_rent_buildings'] = buildings.groupby('parcel_id').impval.sum()/17.9
    far_predictions['current_yearly_rent_buildings'] = far_predictions.current_yearly_rent_buildings.fillna(0)
    far_predictions.current_yearly_rent_buildings = far_predictions.current_yearly_rent_buildings * developer_configuration['land_property_acquisition_cost_factor']
    if zone_args is not None:
        far_predictions.current_yearly_rent_buildings = avgrents.cost_factor*far_predictions.current_yearly_rent_buildings ##Cost scaling happens here
    far_predictions['parcelsize'] = parcels.parcel_sqft

    ###PROFORMA SURFACE CALCULATIONS AND LOOKUPS (TO ARRIVE AT UNCONSTRAINED FARS BY USE)
    # do the lookup in the developer model - this is where the profitability is computed
    dev = spotproforma.Developer(profit_factor=developer_configuration['profit_factor'])
    for form in spotproforma.forms.keys():
        far_predictions[form+'_feasiblefar'], far_predictions[form+'_profit'] = \
                dev.lookup(form,avgrents[spotproforma.uses].as_matrix(),far_predictions.current_yearly_rent_buildings,far_predictions.parcelsize)
    # we now have a far prediction per parcel by allowable building type!

    #################DEVCONSTRAINTS:  Obtain zoning and other development constraints #####
    zoning = dset.fetch('zoning')
    fars = dset.fetch('fars')
    max_parcel_sqft = 200000
    max_far_field = developer_configuration['max_allowable_far_field_name']
    if max_far_field not in parcels.columns:
        parcels = pd.merge(parcels,fars,left_on='far_id',right_index=True)
        if developer_configuration['enforce_environmental_constraints']:
            parcels[max_far_field] = parcels[max_far_field]*(1 - parcels.prop_constrained) #Adjust allowable FAR to account for undevelopable proportion of parcel land
        if developer_configuration['enforce_ugb']:
            parcels[max_far_field][parcels.in_ugb==0] = parcels[max_far_field][parcels.in_ugb==0] * developer_configuration['outside_ugb_allowable_density']
        if developer_configuration['uga_policies']:
            parcels[max_far_field][parcels.in_uga==1] = parcels[max_far_field][parcels.in_ugb==1] * developer_configuration['inside_uga_allowable_density']
        parcels[max_far_field][parcels.parcel_sqft<developer_configuration['min_lot_sqft']] = 0
        parcels[max_far_field][parcels.parcel_sqft>max_parcel_sqft] = 0
    if 'type1' not in parcels.columns:
        parcels = pd.merge(parcels,zoning,left_on='zoning_id',right_index=True)
    ##Scale allowable FARs here if needed
    if zone_args is not None:
        parcels[max_far_field] = parcels[max_far_field]*avgrents.allowable_density_factor

    ####### BUILDING TYPE DICTIONARY #####
    type_d = { 
    'residential': [2,3,20,24],
    'industrial': [9,22],
    'retail': [17,18],
    'office': [5],
    }

    ###MERGE ALLOWABLE DENSITY BY USE WITH FEASIBLE DENSITY BY USE (TAKE MINIMUM) TO ARRIVE AT A PARCEL PREDICTION
    # we have zoning by like 16+ building types and rents/far predictions by 4 more aggregate building types
    # so we have to convert one into the other
    parcel_predictions = pd.DataFrame(index=parcels.index)
    for typ, btypes in type_d.iteritems():
        for btype in btypes:
            # three questions - 1) is type allowed 2) what FAR is allowed 3) is it supported by rents
            if developer_configuration['enforce_allowable_use_constraints']:
                tmp = parcels[parcels['type%d'%btype]==1][[max_far_field]] # is type allowed
                far_predictions['type%d_zonedfar'%btype] = tmp[max_far_field] # at what far
            else:
                far_predictions['type%d_zonedfar'%btype] = parcels[max_far_field]
            # merge zoning with feasibility
            tmp = pd.merge(tmp,far_predictions[[typ+'_feasiblefar']],left_index=True,right_index=True,how='left').set_index(tmp.index)
            # min of zoning and feasibility
            parcel_predictions[btype] = pd.Series(np.minimum(tmp[max_far_field],tmp[typ+'_feasiblefar']),index=tmp.index) 
    parcel_predictions = parcel_predictions.dropna(how='all').sort_index(axis=1)
    for col in parcel_predictions.columns: 
        print col, (parcel_predictions[col]*far_predictions.parcelsize).sum()/1000000.0  ###LIMITING PARCEL PREDICTIONS TO 1MILLION SQFT

    ####SELECTING SITES
    np.random.seed(1)
    p_sample_proportion = .5
    parcel_predictions = parcel_predictions.ix[np.random.choice(parcel_predictions.index, int(len(parcel_predictions.index)*p_sample_proportion),replace=False)]
    parcel_predictions.index.name = 'parcel_id'
    # parcel_predictions.to_csv(os.path.join(misc.data_dir(),'parcel_predictions.csv'),index_col='parcel_id',float_format="%.2f")
    # far_predictions.to_csv(os.path.join(misc.data_dir(),'far_predictions.csv'),index_col='parcel_id',float_format="%.2f")

    #####CALL TO THE DEVELOPER
    newbuildings, price_shifters  = developer.run(dset,hh_zone_diff,emp_zone_diff,parcel_predictions,year=sim_year,
                                 min_building_sqft=developer_configuration['min_building_sqft'],
                                 min_lot_sqft=developer_configuration['min_lot_sqft'],
                                 max_lot_sqft=max_parcel_sqft,zone_args=zone_args) #, elasticity_res=elasticity_res, elasticity_non_res=elasticity_non_res)
                                 
    #####APPLY PRICE SHIFTS (PSEUDO-EQUILIBRATION) [MAKE THIS OPTIONAL]
    print 'Applying price shifts'
    pshift_btypes = []
    pshift_zone = []
    pshift_shift = []
    for item in price_shifters.items():
        pshift_btypes.append(item[0][0])
        pshift_zone.append(item[0][1])
        pshift_shift.append(item[1])
    pshift = pd.DataFrame({'btype':pshift_btypes,'zone':pshift_zone,'shift_amount':pshift_shift})
    buildings['zone_id'] = dset.parcels.zone_id[buildings.parcel_id].values
    buildings['bid'] = buildings.index.values
    buildings = pd.merge(buildings,pshift,left_on=['building_type_id','zone_id'],right_on=['btype','zone'],how='left')
    buildings.shift_amount = buildings.shift_amount.fillna(1.0)
    buildings.unit_price_residential = buildings.unit_price_residential*buildings.shift_amount
    buildings.unit_price_non_residential = buildings.unit_price_non_residential*buildings.shift_amount
    buildings.index = buildings.bid
    
    ##When net residential units is less than 0, need to implement building demolition
    newbuildings = newbuildings[['building_type_id','building_sqft','residential_units','lot_size']]
    newbuildings = newbuildings.reset_index()
    newbuildings.columns = ['parcel_id','building_type_id','bldg_sq_ft','residential_units','land_area']
    newbuildings.residential_units = newbuildings.residential_units.astype('int32')
    newbuildings.land_area = newbuildings.land_area.astype('int32')
    newbuildings.building_type_id = newbuildings.building_type_id.astype('int32')
    newbuildings.parcel_id = newbuildings.parcel_id.astype('int32')
    newbuildings.bldg_sq_ft = np.round(newbuildings.bldg_sq_ft).astype('int32')

    newbuildings['non_residential_sqft'] = 0
    newbuildings.non_residential_sqft[newbuildings.residential_units == 0] = newbuildings.bldg_sq_ft
    newbuildings['improvement_value'] = (newbuildings.non_residential_sqft*100 + newbuildings.residential_units*100000).astype('int32')
    newbuildings['sqft_per_unit'] = 1400
    newbuildings.sqft_per_unit[newbuildings.residential_units>0] = 1000
    newbuildings['stories'] = np.ceil(newbuildings.bldg_sq_ft*1.0/newbuildings.land_area).astype('int32')
    newbuildings['tax_exempt'] = 0
    newbuildings['year_built'] = sim_year
    newbuildings['unit_price_residential'] = 0.0
    newbuildings.unit_price_residential[newbuildings.residential_units>0] = buildings[buildings.unit_price_residential>0].unit_price_residential.median()
    newbuildings['unit_price_non_residential'] = 0.0
    newbuildings.unit_price_non_residential[newbuildings.non_residential_sqft>0] = buildings[buildings.unit_price_non_residential>0].unit_price_non_residential.median()
    newbuildings.unit_price_residential[newbuildings.residential_units>0]  = 200000.0 ###Switched from 100k
    newbuildings.unit_price_non_residential[newbuildings.residential_units==0] = 100.0
    newbuildings['building_sqft_per_job'] = 250.0  #####Need to replace with observed
    newbuildings['non_residential_units'] = (newbuildings.non_residential_sqft/newbuildings.building_sqft_per_job).fillna(0)
    newbuildings['base_year_jobs'] = 0.0
    newbuildings['all_units'] = newbuildings.non_residential_units + newbuildings.residential_units 

    newbuildings.non_residential_sqft = newbuildings.non_residential_sqft.astype('int32')
    newbuildings.tax_exempt = newbuildings.tax_exempt.astype('int32')
    newbuildings.year_built = newbuildings.year_built.astype('int32')
    newbuildings.sqft_per_unit = newbuildings.sqft_per_unit.astype('int32')
    newbuildings = newbuildings.set_index(np.arange(len(newbuildings.index))+np.amax(buildings.index.values)+1)

    buildings = buildings[['building_type_id','improvement_value','land_area','non_residential_sqft','parcel_id','residential_units','sqft_per_unit','stories','tax_exempt','year_built','bldg_sq_ft','unit_price_non_residential','unit_price_residential','building_sqft_per_job','non_residential_units','base_year_jobs','all_units']]
    
    return buildings, newbuildings
