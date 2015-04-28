import statsmodels.api as sm, os
import pandas as pd, numpy as np
from drcog.models import gwr_hedonic
# from GWR_hedonic import estimate_zone as ez,  data_estimation
# from synthicity.utils import misc
# from drcog.models import dataset
# from regression_model_estimation_nr import estimate


def data_simulation(dset, buildings,parcels,fars,zones,establishments):
    bp=buildings
    p=parcels
    f=fars
    z=zones
    e=establishments

    bp['zone_id']= p.zone_id[bp.parcel_id].values
    bp['dist_bus']= (p.dist_bus[bp.parcel_id].values)
    bp['ln_dist_bus']=np.log(bp['dist_bus'])
    bp['centroid_x']= p.centroid_x[bp.parcel_id].values
    bp['centroid_y']= p.centroid_y[bp.parcel_id].values
    bp['year20']=pd.DataFrame((bp['year_built']>1995)*(bp['year_built']<2000)).applymap(lambda x: 1 if x else 0)
    bp['year10']=pd.DataFrame((bp['year_built']>1999)*(bp['year_built']<2006)).applymap(lambda x: 1 if x else 0)
    bp['year0']=pd.DataFrame((bp['year_built']>2005)).applymap(lambda x: 1 if x else 0)

    f['far_id']=f.index
    p['far']=f.far[p.far_id].values
    bp['far']= p.far[bp.parcel_id].values
    del f['far_id']
    del  p['far']

    bp['high_land_area']=pd.DataFrame((bp['land_area']>7000)).applymap(lambda x: 1 if x else 0)
    bp['ln_nres_sqft']=np.log(bp['non_residential_sqft'])
    bp['ln_res_sqft']=np.log(bp['bldg_sq_ft'])

    bp['unit_price_res_sqft']=bp[bp['bldg_sq_ft']>0]['unit_price_residential']/bp['bldg_sq_ft']
    e['zone_id'] = bp.zone_id[e.building_id].values
    z['emp_sector_mean'] = e.groupby('zone_id').employees.mean()
    del  e['zone_id']

    bp['emp_sector_10']=z.emp_sector_mean[bp.zone_id].values
    bp['ln_emp_10']=np.log(bp['emp_sector_10'])
    del  z['emp_sector_mean']
    del  bp['emp_sector_10']

    z['nr_stories_mean']=bp.groupby('zone_id').stories.sum()
    z['stories_30_10']=dset.compute_range(z['nr_stories_mean'], 15)-\
        dset.compute_range(z['nr_stories_mean'], 5, agg=np.sum)

    bp['stories_10']=z.nr_stories_mean[bp.zone_id].values
    bp['ln_stories_zone']=-bp['stories']+bp['stories_10']
    del  bp['stories_10']
    del z['nr_stories_mean']

    bp['stories_30_10']=z.stories_30_10[bp.zone_id].values
    bp['ln_stories_out']=bp['stories_30_10']
    del  z['stories_30_10']
    del bp['stories_30_10']

    del e
    del p
    dset.d['buildings']=bp
    dset.d['zones']=z
    return dset


def data_simulation_bldg(data, bid ):
    bp=data.buildings
    z=data.zones
    z['r_sqft_mean']=bp[(bp['building_type_id']==bid)*(bp['bldg_sq_ft']>0)].groupby('zone_id').ln_res_sqft.sum()
    z['ln_r_sqft_mean']=np.log( z['r_sqft_mean'])

    z['r_sqft_30_10']=data.compute_range(z[np.isfinite(z['ln_r_sqft_mean'])]['ln_r_sqft_mean'], 15, agg=np.sum)- \
            data.compute_range(z[np.isfinite(z['ln_r_sqft_mean'])]['ln_r_sqft_mean'], 5, agg=np.sum)
    del  z['ln_r_sqft_mean']
    bp['r_sqft_30_10']=z.r_sqft_30_10[bp.zone_id].values
    bp['r_sqft_10']=z.r_sqft_mean[bp.zone_id].values
    bp['ln_sqft_zone']=-np.log(bp.bldg_sq_ft)+bp['r_sqft_10']
    bp['ln_sqft_out']=-np.log(bp.bldg_sq_ft)+bp['r_sqft_30_10']
    del z['r_sqft_30_10']
    del bp['r_sqft_10']
    del bp['r_sqft_30_10']
    del  z['r_sqft_mean']
    data.d['buildings']=bp
    return data



def simulate(data, dset_coeff,outvarname, ind_vars,year, segment):
    zrent=[]

    coeff=dset_coeff
    dset=data_simulation(data, data.buildings,data.parcels,data.fars,data.zones,data.establishments)

    for bid in segment:

        data_simulation_bldg(dset, bid )
        coeffb=coeff[coeff['bid']==bid]

        b=dset.buildings[dset.buildings['building_type_id']==bid]
        est_data=pd.DataFrame(index=b.index)

        for varname in ind_vars:
            est_data[varname]=b[varname]
        est_data = sm.add_constant(est_data,prepend=False)
        est_data['zone_id']=b['zone_id']


        print "Generating Rents on buildings"
        grouped=est_data.groupby('zone_id')

        for name,group in grouped:
            vec=coeffb[coeffb['zone_id']==name]
            del vec['zone_id']
            del vec['bid']
            del group['zone_id']
            vecT=vec.T



            rents=group.dot(np.array(vecT)).astype('f4')

            zrent.append(rents)
    rent=pd.concat(zrent)
    dset.buildings[outvarname] = rent.reindex(dset.buildings.index)
    dset.buildings[outvarname] =np.exp(dset.buildings[outvarname] )

    dset.store_attr(outvarname,year,dset.buildings[outvarname])

    return dset



