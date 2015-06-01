import statsmodels.api as sm
import pandas as pd, numpy as np, os
from synthicity.utils import misc
import pysal
from census_hedonic import data_zone,data_zone_census

def simulate_residential_zone(dset, depvar, fixedeffect):

    # Construct the data for simulation
    zones=data_zone(dset,dset.buildings, dset.parcels,dset.zones,dset.establishments)
    data=data_zone_census(zones)

    # Extract coeff

    ind_vars = dset.coeffs_res[('coeff_residential', 'fnames')][np.invert(dset.coeffs_res[('coeff_residential', 'fnames')].isnull().values)].values.tolist()

    # Remove inf or nan
    for varname in ind_vars:
            if varname != 'const':
                try:
                    data=data[np.isfinite(data[varname])]
                except:
                    pass


    # Construct the RHS variables
    est_data = pd.get_dummies(data[fixedeffect], prefix='sdis')
    del est_data['sdis_8']

    for varname in ind_vars:
            if varname != 'const':
                try:
                    est_data[varname] = data[varname]
                except:
                    pass

    est_data['cons']=1



    # Price simulation at the zonal level
    print "Generating rents on residential buildings"
    vec = dset.load_coeff_res("coeff_residential")

    vec = np.reshape(vec,(vec.size,1))
    rents_zonal = est_data.dot(vec).astype('f4')


    rents=pd.DataFrame(rents_zonal, index=data.index)
    rents.columns=['zonal_rent']
    rents['zonal_rent']=np.exp(rents['zonal_rent'])*data['median_income']


    return rents


def filling(int,data, var,W):
    return data[np.in1d(data['ind'], W[int])][var].mean()


def missing_zones(dset,rents,k):

    #Zones dataset
    z=dset.zones
    z['zone_id']=z.index
    rents=pd.merge(z, rents, left_on='zone_id', right_index=True, how='outer')
    rents['ind']=np.arange(len(rents.index))

    # Construct nearest neighbors
    geo=pd.DataFrame(z['zonecentroid_x'])
    geo['zonecentroid_y']=z['zonecentroid_y']
    geo=np.array(geo)
    n=len(geo)

    w=pysal.knnW(geo, k=k)
    wneigh=np.zeros((n,k))
    for i in range(0,n):
        wneigh[i,:]=w.neighbors[i]


    ind=np.array(rents[np.isnan(rents['zonal_rent'])].ind)

    rents.loc[np.isnan(rents['zonal_rent']),'zonal_rent']=rents.ind.apply(filling, args=(rents, 'zonal_rent', wneigh))

    return pd.DataFrame(rents['zonal_rent'], index=rents.index)


def simulate_residential(dset, depvar, fixedeffect,k,year):

    # Building tables with zone_id
    b=dset.buildings

    if ~np.in1d(list(b.columns.values), 'zone_id').all():
        p=dset.parcels
        if p.index.name != 'parcel_id':
            p = p.set_index(p['parcel_id'])
        b['zone_id']=p.zone_id[b.parcel_id].values



    # Add zonal prices to building tables
    rent_zone=simulate_residential_zone(dset, depvar, fixedeffect)
    rent_zone=missing_zones(dset,rent_zone,k)

    b=pd.merge(b, rent_zone, left_on='zone_id', right_index=True)
    b=b.reindex(dset.buildings.index)

    dset.buildings['zonal_rent']=b['zonal_rent']

    # Compute price per sqft:
    dset.buildings[depvar]=dset.buildings[dset.buildings.residential_units>0].zonal_rent/(dset.buildings[dset.buildings.residential_units>0].bldg_sq_ft)

    dset.buildings['unit_price_residential']=dset.buildings[dset.buildings['residential_units']>0].zonal_rent
    del dset.buildings['zonal_rent']

    dset.store_attr(depvar,year,dset.buildings[depvar])



"""
from synthicity.utils import misc
from drcog.models import dataset
dset = dataset.DRCOGDataset(os.path.join(misc.data_dir(),'drcog.h5'))


print dset.buildings[dset.buildings['residential_units']>0]
coeff_store = pd.HDFStore(os.path.join(misc.data_dir(),'coeffs_res.h5'))
dset.coeffs_res = coeff_store.coeffs_res.copy()
coeff_store.close()


r=simulate_residential(dset, 'unit_price_per_sqft', 'school_district_id', 30, 2010)

b=dset.fetch('buildings')
print b
"""
"""
def simulate(dset,year,output_varname = 'price',simulation_table = 'buildings',output_names=None,agents_groupby='building_type_id', segment_ids=[2,3,20,24]):

    output_csv, output_title, coeff_name = output_names

    choosers = dset.fetch(simulation_table)
    choosers=choosers[np.in1d(choosers.building_type_id,segment_ids)]



    choosers['ln_county_income_mean']= choosers['ln_county_income_mean']+ (year-2010)*np.log(1+0.00)
    choosers['ln_mean_income']= choosers['ln_mean_income']+ (year-2010)*np.log(1+0.00)
    segments = choosers.groupby(agents_groupby)


    simrents = []

    for name, segment in segments:
        tmp_outcsv, tmp_outtitle, tmp_coeffname = output_csv%name, output_title%name, coeff_name%name
        ind_vars = dset.coeffs[(tmp_coeffname, 'fnames')][np.invert(dset.coeffs[(tmp_coeffname, 'fnames')].isnull().values)].values.tolist()
        est_data = pd.DataFrame(index=segment.index)
        for varname in ind_vars:
            if varname != 'const':
                est_data[varname] = segment[varname]

                ###XG: NA should NOT be replaced by zeros, otherwise it messes up the regression
                # #est_data.fillna(0, inplace=True)
            else:
                est_data = sm.add_constant(est_data,prepend=False)

        print "Generating rents on %d buildings" % (est_data.shape[0])
        vec = dset.load_coeff(tmp_coeffname)

        vec = np.reshape(vec,(vec.size,1))

        try:
            rents = est_data.dot(vec).astype('f4')

        except:

            est_data['const'] = 1
            rents = pd.DataFrame({'rents':np.transpose(np.dot(est_data,vec).astype('f4'))[0]},index=est_data.index)
        rents = (1.00)**(year-2010)*rents.apply(np.exp)

        simrents.append(rents[rents.columns[0]])
    simrent= pd.concat(simrents)


    dset.buildings[output_varname] = simrent.reindex(dset.buildings.index)


    #### XG: back to unit price using sqft price and sqft
    if  segment_ids==[2,3,20,24]:
        dset.buildings['unit_price_residential']=dset.buildings[output_varname]*dset.buildings['bldg_sq_ft']


    dset.store_attr(output_varname,year,dset.buildings[output_varname])

    return (choosers, est_data, vec, simrent)



    simrent=pd.DataFrame(dset.buildings.parcel_id)
    simrent.columns=['parcel_id']
    simrent['rents']= pd.concat(simrents)




    simrent['zone_id']= dset.parcels.zone_id[simrent.parcel_id].values
    simrent['bldg_sq_ft']=dset.buildings.bldg_sq_ft
    simrent['building_type_id']=dset.buildings.building_type_id

    simnull=pd.DataFrame(simrent[(simrent.bldg_sq_ft>0)*(np.in1d(simrent.building_type_id,[5,9, 17, 18, 22]))].groupby('parcel_id').rents.mean())
    simnull.columns=['srent_nr']
    simrent=pd.merge(simrent, simnull, left_on='parcel_id', right_index=True)

    simnull=pd.DataFrame(simrent[(simrent.bldg_sq_ft>0)*(np.in1d(simrent.building_type_id,[2,3,20,24]))].groupby('parcel_id').rents.mean())
    simnull.columns=['srent_r']
    simrent=pd.merge(simrent, simnull, left_on='parcel_id', right_index=True)

    simrent['price']=simrent['rents']*simrent['bldg_sq_ft']
    simnull=pd.DataFrame(simrent[(simrent.bldg_sq_ft>0)].groupby('parcel_id').price.mean())
    simnull.columns=['sprice']
    simrent=pd.merge(simrent, simnull, left_on='parcel_id', right_index=True)

    #XG: For empty places, price per sqft computed as the average price per square foot on the zone

    dset.buildings['rents'] = simrent['rents']

    dset.buildings['srent_r']=simrent['srent_r']
    dset.buildings['srent_nr']=simrent['srent_nr']


    dset.buildings['sprice']=simrent['sprice']
    dset.buildings['price']=simrent['price']

    if  segment_ids==[2,3,20,24]:
        dset.buildings[output_varname]=(dset.buildings.rents)*((dset.buildings['bldg_sq_ft']>0).astype('int32'))\
                                       #+ (dset.buildings.srent_r)*((dset.buildings['bldg_sq_ft']==0).astype('int32'))
    else:
        dset.buildings[output_varname]=(dset.buildings.rents)*((dset.buildings['bldg_sq_ft']>0).astype('int32')) \
                                       #+ (dset.buildings.srent_nr)*((dset.buildings['bldg_sq_ft']==0).astype('int32'))

    if  segment_ids==[2,3,20,24]:
        dset.buildings['unit_price_residential']=(dset.buildings.price)*((dset.buildings['bldg_sq_ft']>0).astype('int32'))\
                                                 #+ (dset.buildings.sprice)*((dset.buildings['bldg_sq_ft']==0).astype('int32'))

    dset.buildings[output_varname]=(dset.buildings.rents)

    del dset.buildings['rents']
    del dset.buildings['srent_r']
    del dset.buildings['srent_nr']
    del dset.buildings['price']
    del dset.buildings['sprice']



    print dset.buildings[output_varname].mean()

    dset.store_attr(output_varname,year,dset.buildings[output_varname])

    return (choosers, est_data, vec, simrent)
"""""

