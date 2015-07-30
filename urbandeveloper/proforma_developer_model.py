import pandas as pd, numpy as np
import sys, time, random, string, os
from synthicity.utils import misc
from urbandeveloper import spotproforma, new_developer
import sys
def run(dset,hh_zone1,emp_zone1,developer_configuration,sim_year):
    #Record post-demand-model change in zone-level household/job totals
    hh = dset.fetch('households')
    e = dset.fetch('establishments')
    buildings = dset.fetch('buildings')
    parcels = dset.parcels
    if parcels.index.name != 'parcel_id':
        parcels = parcels.set_index(parcels['parcel_id'])
    buildings['zone_id'] = parcels.zone_id[buildings.parcel_id].values

    #e['zone_id'] = buildings.zone_id[e.building_id].values
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
        dset.zones.loc[dset.zones.county == 'Adams', "county_id"] = 8001  # corrected chained index error
        dset.zones.loc[dset.zones.county == 'Arapahoe', "county_id"] = 8005  # corrected chained index error
        dset.zones.loc[dset.zones.county == 'Boulder', "county_id"] = 8013  # corrected chained index error
        dset.zones.loc[dset.zones.county == 'Broomfield', "county_id"] = 8014  # corrected chained index error
        dset.zones.loc[dset.zones.county == 'Clear Creek', "county_id"] = 8019  # corrected chained index error
        dset.zones.loc[dset.zones.county == 'Denver', "county_id"] = 8031  # corrected chained index error
        dset.zones.loc[dset.zones.county == 'Douglas', "county_id"] = 8035  # corrected chained index error
        dset.zones.loc[dset.zones.county == 'Elbert', "county_id"] = 8039  # corrected chained index error
        dset.zones.loc[dset.zones.county == 'Gilpin', "county_id"] = 8047  # corrected chained index error
        dset.zones.loc[dset.zones.county == 'Jefferson', "county_id"] = 8059  # corrected chained index error
        dset.zones.loc[dset.zones.county == 'Weld', "county_id"] = 8123  # corrected chained index error
        zone_args['cid'] = dset.zones.county_id
        pd.set_option('display.max_rows', 1000)

        ##Loading/applying county calib factors to scale the zone args
        county_args = pd.read_csv(os.path.join(misc.data_dir(),'county_calib.csv')).set_index('county_id')
        zone_args['county_id']=zone_args['cid']
        zone_args = pd.merge(zone_args,county_args,left_on='county_id',right_index=True)

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
    buildings = buildings[['building_type_id','improvement_value','land_area','non_residential_sqft','parcel_id','residential_units','sqft_per_unit','stories','tax_exempt','year_built','bldg_sq_ft','unit_price_non_residential','unit_price_residential','building_sqft_per_job','non_residential_units','base_year_jobs','all_units', 'unit_price_res_sqft']]
    buildings.loc[:, "zone_id"] = parcels.zone_id[buildings.parcel_id].values  #  corrected chain index error

    res_buildings = buildings[buildings.unit_price_residential>0]
    nonres_buildings = buildings[buildings.unit_price_non_residential>0]
    nonres_buildings_office = nonres_buildings[nonres_buildings.building_type_id==5]
    nonres_buildings_retail = nonres_buildings[np.in1d(nonres_buildings.building_type_id,[17,18])]
    nonres_buildings_industrial = nonres_buildings[np.in1d(nonres_buildings.building_type_id,[9,22])]

    # Price now are in price/sqft
    #### XG: define residential price only on types 2,3, 20, 24 and non-residential 5, 9, 17,18,22
    zone_args['zone_id']=zone_args.index
    res_buildings.loc[:, "resprice_sqft"] = res_buildings[(res_buildings.bldg_sq_ft>0)*(np.in1d(res_buildings.building_type_id,[2,3,20,24]))].unit_price_res_sqft  # corrected chain index error
    zonal_resprice_sqft = pd.DataFrame(res_buildings[(res_buildings.bldg_sq_ft>0)*(np.in1d(res_buildings.building_type_id,[2,3,20,24]))].groupby('zone_id').resprice_sqft.mean())
    zonal_resprice_sqft.columns=['resrent']
    zone_args=pd.merge(zone_args,zonal_resprice_sqft, left_on='zone_id', right_index=True, how='outer')
    zonal_nonresprice_office = pd.DataFrame(nonres_buildings_office[nonres_buildings_office.non_residential_sqft>0].groupby('zone_id').unit_price_non_residential.mean())
    zonal_nonresprice_office.columns=['nonresrent_office']
    zone_args=pd.merge(zone_args,zonal_nonresprice_office, left_on='zone_id', right_index=True, how='outer')
    zonal_nonresprice_retail = pd.DataFrame(nonres_buildings_retail[ nonres_buildings_retail.non_residential_sqft>0].groupby('zone_id').unit_price_non_residential.mean())
    zonal_nonresprice_retail.columns=['nonresrent_retail']
    zone_args=pd.merge(zone_args,zonal_nonresprice_retail, left_on='zone_id', right_index=True, how='outer')
    zonal_nonresprice_industrial = pd.DataFrame(nonres_buildings_industrial[ nonres_buildings_industrial.non_residential_sqft>0].groupby('zone_id').unit_price_non_residential.mean())
    zonal_nonresprice_industrial.columns=['nonresrent_industrial']
    zone_args=pd.merge(zone_args,zonal_nonresprice_industrial, left_on='zone_id', right_index=True, how='outer')
    zone_args['resrent']=zone_args['resrent']*zone_args.res_price_factor
    zone_args['nonresrent_office']=zone_args['nonresrent_office']* zone_args.nonres_price_factor
    zone_args['nonresrent_retail']=zone_args['nonresrent_retail']* zone_args.nonres_price_factor
    zone_args['nonresrent_industrial']=zone_args['nonresrent_industrial']* zone_args.nonres_price_factor
    zonal_avg_rents= zone_args[['resrent', 'nonresrent_office', 'nonresrent_retail','nonresrent_industrial','cost_factor','allowable_density_factor']]
    zonal_avg_rents.loc[:, "zone_id"] = zonal_avg_rents.index  #  corrected chain index error
    zonal_avg_rents.loc[:, 'county_id'] = dset.zones.county_id[zonal_avg_rents['zone_id']].values  # corrected chain index error
    pd.set_option('display.max_rows', len(dset.zones.index))
    del  zonal_avg_rents['county_id']
    del zonal_avg_rents['zone_id']

    """
    res_buildings['resprice_sqft'] = res_buildings[(res_buildings.bldg_sq_ft>0)*(np.in1d(res_buildings.building_type_id,[2,3,20,24]))].unit_price_res_sqft
    zonal_resprice_sqft = pd.DataFrame(res_buildings[(res_buildings.bldg_sq_ft>0)*(np.in1d(res_buildings.building_type_id,[2,3,20,24]))].groupby('zone_id').resprice_sqft.mean())
    zonal_nonresprice_office = pd.DataFrame(nonres_buildings_office[nonres_buildings_office.non_residential_sqft>0].groupby('zone_id').unit_price_non_residential.mean())
    zonal_avg_rents=pd.join(zonal_resprice_sqft, zonal_nonresprice_office, how='outer')
    print  zonal_avg_rents
    sys.exit('beurk')
    zonal_nonresprice_retail = pd.DataFrame(nonres_buildings_retail[ nonres_buildings_retail.non_residential_sqft>0].groupby('zone_id').unit_price_non_residential.mean())
    zonal_avg_rents=pd.join( zonal_nonresprice_retail, zonal_avg_rents, how='outer')
    zonal_nonresprice_industrial = nonres_buildings_industrial[ nonres_buildings_industrial.non_residential_sqft>0].groupby('zone_id').unit_price_non_residential.mean()
    zonal_resrent = zonal_resprice_sqft
    zonal_nonresrent_office = zonal_nonresprice_office
    zonal_nonresrent_retail = zonal_nonresprice_retail
    zonal_nonresrent_industrial = zonal_nonresprice_industrial

    if zone_args is not None:  #####Make sure no nulls in the prices either...
        zonal_resrent = zonal_resrent * zone_args.res_price_factor
        print zonal_resrent
        zonal_nonresrent_office = zonal_nonresprice_office * zone_args.nonres_price_factor
        zonal_nonresrent_retail = zonal_nonresprice_retail * zone_args.nonres_price_factor
        zonal_nonresrent_industrial = zonal_nonresprice_industrial * zone_args.nonres_price_factor
        zonal_avg_rents = pd.DataFrame({'resrent':zonal_resrent,'nonresrent_office':zonal_nonresrent_office,'nonresrent_retail':zonal_nonresrent_retail,'nonresrent_industrial':zonal_nonresrent_industrial,'cost_factor':zone_args.cost_factor,'allowable_density_factor':zone_args.allowable_density_factor}, index=zonal_resrent.index)
    else:
        zonal_avg_rents = pd.DataFrame({'resrent':zonal_resrent,'nonresrent_office':zonal_nonresrent_office,'nonresrent_retail':zonal_nonresrent_retail,'nonresrent_industrial':zonal_nonresrent_industrial})
    zonal_avg_rents['zone_id']=zonal_avg_rents.index
    zonal_avg_rents['county_id']=dset.zones.county_id[zonal_avg_rents['zone_id']].values
    pd.set_option('display.max_rows', len(dset.zones.index))
    print zonal_avg_rents[ zonal_avg_rents['county_id']==8123].zone_id
    del  zonal_avg_rents['county_id']
    del zonal_avg_rents['zone_id']
    """
    avgrents = pd.merge(parcels,zonal_avg_rents,left_on='zone_id',right_index=True,how='left')
    avgrents['residential'] = avgrents.resrent
    avgrents['office'] = avgrents.nonresrent_office
    avgrents['retail'] = avgrents.nonresrent_retail
    avgrents['industrial'] = avgrents.nonresrent_industrial

    if zone_args is not None:
        avgrents = avgrents[['residential','office','retail','industrial','cost_factor','allowable_density_factor', 'county_id']]
    else:
        avgrents = avgrents[['residential','office','retail','industrial']]
    avgrents = avgrents.fillna(.1)

    #avgrents.residential[np.isinf(avgrents.residential)] = .2
    avgrents.loc[avgrents.residential < .2, "residential"] = .2  # corrected chain index error
    avgrents.loc[avgrents.office < 1, "office"] = 1  # corrected chain index error
    avgrents.loc[avgrents.retail < 1, "retail"] = 1  # corrected chain index error
    avgrents.loc[avgrents.industrial < 1, "industrial"] = 1  # corrected chain index error

    ####################GET PARCEL LEVEL ATTRIBUTES
    #### XG: retain old square footage as it is used to compute average
    buildings.loc[:, 'bldg_sq_ft2'] = buildings['bldg_sq_ft']  # corrected chain index error
    buildings.loc[:, 'bldg_sq_ft'] = buildings.non_residential_sqft + buildings.residential_units*buildings.sqft_per_unit  # corrected chain index error
    #buildings['impval'] = buildings.non_residential_sqft*buildings.unit_price_non_residential + buildings.residential_units*buildings.unit_price_residential
    buildings.loc[:, 'impval'] = 0  # corrected chain index error
    buildings.loc[buildings.residential_units*buildings.unit_price_residential>0,'impval'] = buildings.residential_units*buildings.unit_price_residential
    buildings.loc[buildings.non_residential_sqft*buildings.unit_price_non_residential >0,'impval']=buildings['impval']+ buildings.non_residential_sqft*buildings.unit_price_non_residential
    far_predictions = pd.DataFrame(index=parcels.index)
    #far_predictions['current_yearly_rent_buildings'] = buildings.groupby('parcel_id').impval.sum()/17.9
    far_predictions['current_yearly_rent_buildings'] = buildings.groupby('parcel_id').impval.sum()
    far_predictions['current_yearly_rent_buildings'] = far_predictions.current_yearly_rent_buildings.fillna(0)
    far_predictions.current_yearly_rent_buildings = far_predictions.current_yearly_rent_buildings * developer_configuration['land_property_acquisition_cost_factor']
    far_predictions['county_id']=parcels.county_id[far_predictions.index].values
    print  far_predictions[far_predictions['current_yearly_rent_buildings']>0].groupby('county_id').current_yearly_rent_buildings.mean()


    if zone_args is not None:
        #far_predictions.current_yearly_rent_buildings = avgrents.cost_factor*far_predictions.current_yearly_rent_buildings ##Cost scaling happens here
        far_predictions.current_yearly_rent_buildings = far_predictions.current_yearly_rent_buildings
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
        parcels.loc[parcels.parcel_sqft < developer_configuration['min_lot_sqft'], "max_far_field"] = 0  # fixed chained index error
        parcels.loc[parcels.parcel_sqft > max_parcel_sqft, "max_far_field"] = 0  # fixed chained indexing error
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

    parcel_predictions['county_id']=parcels.county_id

    for typ, btypes in type_d.iteritems():
        for btype in btypes:
            # three questions - 1) is type allowed 2) what FAR is allowed 3) is it supported by rents
            if developer_configuration['enforce_allowable_use_constraints']:
                tmp = parcels[parcels['type%d'%btype]==1][[max_far_field]]

                 # is type allowed
                far_predictions['type%d_zonedfar'%btype] = tmp[max_far_field] # at what far
            else:
                far_predictions['type%d_zonedfar'%btype] = parcels[max_far_field]
            # merge zoning with feasibility
            tmp.index.name='parcel_id'
            tmp = pd.merge(tmp,far_predictions[[typ+'_feasiblefar']],left_index=True,right_index=True,how='left').set_index(tmp.index)
            # min of zoning and feasibility

            parcel_predictions[btype] = pd.Series(np.minimum(tmp[max_far_field],tmp[typ+'_feasiblefar']),index=tmp.index)
            #avgrents2=avgrents.ix[parcels['type%d'%btype]==1]
            #profit=dev.profit(typ,avgrent2s[spotproforma.uses].as_matrix(),far_predictions.current_yearly_rent_buildings,parcel_prediction[btype])

            #print profit
            #parcel_predictions[btype+'_profit']=pd.Series(profit,index=tmp.index)
    parcel_predictions = parcel_predictions.dropna(how='all').sort_index(axis=1)


    for col in parcel_predictions.columns:
        print col, (parcel_predictions[col]*far_predictions.parcelsize).sum()/1000000.0  ###LIMITING PARCEL PREDICTIONS TO 1MILLION SQFT

    ####SELECTING SITES
    np.random.seed(1)
    p_sample_proportion = .5
    parcel_predictions = parcel_predictions.ix[np.random.choice(parcel_predictions.index, int(len(parcel_predictions.index)*p_sample_proportion),replace=False)]
    parcel_predictions.index.name = 'parcel_id'


    parcel_predictions.to_csv(os.path.join(misc.data_dir(),'parcel_predictions.csv'),index_col='parcel_id',float_format="%.2f")
    # far_predictions.to_csv(os.path.join(misc.data_dir(),'far_predictions.csv'),index_col='parcel_id',float_format="%.2f")

    #####CALL TO THE DEVELOPER
    newbuildings, price_shifters  = new_developer.run(dset,hh_zone_diff,emp_zone_diff,parcel_predictions,year=sim_year,
                                 min_building_sqft=developer_configuration['min_building_sqft'],
                                 min_lot_sqft=developer_configuration['min_lot_sqft'],
                                 max_lot_sqft=max_parcel_sqft,zone_args=zone_args, tot_sqft=dset.zones[['residential_sqft_zone','non_residential_sqft_zone']])
                                 
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
    buildings['zone_id'] = parcels.loc[buildings.parcel_id, "zone_id"].values
    buildings['bid'] = buildings.index.values
    buildings = pd.merge(buildings,pshift,left_on=['building_type_id','zone_id'],right_on=['btype','zone'],how='left')
    buildings.shift_amount = buildings.shift_amount.fillna(1.0)
    buildings.unit_price_residential = buildings.unit_price_residential*buildings.shift_amount
    #buildings.unit_price_non_residential = buildings.unit_price_non_residential*buildings.shift_amount
    buildings.index = buildings.bid
    
    ##When net residential units is less than 0, need to implement building demolition
    newbuildings = newbuildings[['zone_id','building_type_id',
                                 'building_sqft','residential_units','lot_size']]
    #print newbuildings.building_sqft
    newbuildings = newbuildings.reset_index()

    newbuildings.columns = ['parcel_id','zone_id','building_type_id','bldg_sq_ft','residential_units','land_area']
    newbuildings.parcel_id = newbuildings.parcel_id.astype('int32')
    #newbuildings['county_id']=parcel_predictions.county_id[newbuildings.parcel_id].values  # why is this here?

    #print newbuildings[newbuildings.residential_units == 0].groupby('county_id').bldg_sq_ft.sum()
    newbuildings.residential_units = newbuildings.residential_units.astype('int32')
    newbuildings.land_area = newbuildings.land_area.astype('int32')
    newbuildings.building_type_id = newbuildings.building_type_id.astype('int32')
    newbuildings.parcel_id = newbuildings.parcel_id.astype('int32')
    newbuildings.bldg_sq_ft = np.round(newbuildings.bldg_sq_ft).astype('int32')
    newbuildings.bldg_sq_ft2 = np.round(newbuildings.bldg_sq_ft).astype('int32')

    newbuildings['non_residential_sqft'] = 0
    newbuildings.loc[newbuildings.residential_units == 0, "non_residential_sqft"] = newbuildings.bldg_sq_ft
    newbuildings['improvement_value'] = (newbuildings.non_residential_sqft*100 + newbuildings.residential_units*100000).astype('int32')
    newbuildings['sqft_per_unit'] = 1400
    newbuildings.loc[newbuildings.residential_units>0, "sqft_per_unit"] = 1000
    newbuildings['stories'] = np.ceil(newbuildings.bldg_sq_ft*1.0/newbuildings.land_area).astype('int32')
    newbuildings['tax_exempt'] = 0
    newbuildings['year_built'] = sim_year
    newbuildings['unit_price_residential'] = 0.0
    newbuildings.loc[newbuildings.residential_units>0, "unit_price_residential"] = buildings[buildings.unit_price_residential>0].unit_price_residential.median()

    newbuildings['unit_price_res_sqft'] = 0.0
    newbuildings.loc[newbuildings.residential_units>0, "unit_price_res_sqft"] = buildings[buildings.unit_price_res_sqft>0].unit_price_res_sqft.median()

    newbuildings['unit_price_non_residential'] = 0.0
    newbuildings.loc[newbuildings.non_residential_sqft>0, "unit_price_non_residential"] = buildings[buildings.unit_price_non_residential>0].unit_price_non_residential.median()

    ##### XG: originally, impose exogenous prices for new buildings. Now impose average county price
    #newbuildings['county_id'] = dset.parcels.county_id[newbuildings.parcel_id].values  # improper join - index incorrect
    newbuildings['county_id'] = parcels.loc[newbuildings.parcel_id, "county_id"].values

    #buildings['county_id'] = dset.parcels.county_id[buildings.parcel_id].values  # improper join - index incorrect
    buildings['county_id'] = parcels.loc[buildings.parcel_id, "county_id"].values
    u=pd.DataFrame(buildings[(buildings.bldg_sq_ft2>0)*(np.in1d(buildings.building_type_id,[2,3,20,24]))].groupby('county_id').unit_price_res_sqft.mean())
    u.columns=['res_price_county']

    newbuildings=pd.merge(newbuildings, u, left_on='county_id', right_index=True)

    u=pd.DataFrame(buildings[(buildings.non_residential_sqft>0)*(np.in1d(buildings.building_type_id,[5,9,17,18,22]))].groupby('county_id').unit_price_non_residential.mean())
    u.columns=['nres_price_county']
    newbuildings=pd.merge(newbuildings, u, left_on='county_id', right_index=True)

    u=pd.DataFrame(buildings.groupby('county_id').unit_price_residential.mean())
    u.columns=['unit_res_price_county']
    newbuildings=pd.merge(newbuildings, u, left_on='county_id', right_index=True)

    newbuildings.loc[(newbuildings.bldg_sq_ft>0)*(np.in1d(newbuildings.building_type_id,[2,3,20,24])), "unit_price_residential"] = newbuildings.unit_res_price_county
    newbuildings.loc[(newbuildings.bldg_sq_ft>0)*(np.in1d(newbuildings.building_type_id,[2,3,20,24])), "unit_price_res_sqft"] = newbuildings.res_price_county
    newbuildings.loc[(newbuildings.non_residential_sqft>0)*(np.in1d(newbuildings.building_type_id,[5,9,17,18,22])), "unit_price_non_residential"] = newbuildings.nres_price_county
    #print newbuildings[(np.in1d(newbuildings.building_type_id,[2,3,20,24]))*(newbuildings['bldg_sq_ft']>0)].groupby('county_id').unit_price_res_sqft.mean()
    #### end XG



    newbuildings['building_sqft_per_job'] = 250.0  #####Need to replace with observed
    newbuildings['non_residential_units'] = (newbuildings.non_residential_sqft/newbuildings.building_sqft_per_job).fillna(0)
    newbuildings['base_year_jobs'] = 0.0
    newbuildings['all_units'] = newbuildings.non_residential_units + newbuildings.residential_units 

    newbuildings.non_residential_sqft = newbuildings.non_residential_sqft.astype('int32')
    newbuildings.tax_exempt = newbuildings.tax_exempt.astype('int32')
    newbuildings.year_built = newbuildings.year_built.astype('int32')
    newbuildings.sqft_per_unit = newbuildings.sqft_per_unit.astype('int32')
    newbuildings = newbuildings.set_index(np.arange(len(newbuildings.index))+np.amax(buildings.index.values)+1)

    buildings = buildings[['zone_id','building_type_id','improvement_value','land_area','non_residential_sqft','parcel_id','residential_units','sqft_per_unit','stories','tax_exempt','year_built','bldg_sq_ft','bldg_sq_ft2','unit_price_non_residential','unit_price_residential','building_sqft_per_job','non_residential_units','base_year_jobs','all_units', 'unit_price_res_sqft']]
    
    return buildings, newbuildings

