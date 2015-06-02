import numpy as np, pandas as pd, os
from synthicity.utils import misc
from drcog.models import dataset

np.random.seed(1)
import statsmodels.api as sm
import pysal
import matplotlib.pyplot as plt
import statsmodels

## This code estimate a hedonic model for residential units. It is using data from the census for household characteristics (income), prices, average year of move... and
## buildings data aggregated at the zonal level. The model regresses the ratio of price over income (at the zonal level) on zonal characteristics. The ratio price/income allows to correct for some of the
## heteroskedasticity, which may generate biases when taking the log. On the right hand side, income (later, age) is instrumented by the year of move, as different the year
## of move implies different credit and housing markets conditions at purchasing time.


## The code has two parts:
##   ------ Part 1: Generate the data for extimation
##   ------ Part 2: Regression and coefficient export

### PART 1: Construction of the table for estimation
#### PART 1.a.: Buildings characteristics at the zonal level:

def data_zone(dset,buildings, parcels,zones,establishments):
    b=buildings
    p=parcels
    if p.index.name != 'parcel_id':
        p = p.set_index(parcels['parcel_id'])
    z=zones
    e=establishments

    ### neighborhood (right now zone ) characteristics
    b['zone_id']=p.zone_id[b.parcel_id].values

    e['zone_id'] = b.zone_id[e.building_id].values
    z['zone_id']=z.index


    pp=pd.DataFrame(p['zone_id'], index=p.index)
    pp['county_id']=p['county_id']
    pp.drop_duplicates(['zone_id' ], inplace=True)
    pp=pp.reset_index(pp.zone_id)

    z=pd.merge(z, pp, left_on='zone_id', right_index=True)



    z['emp']=e.groupby('zone_id').employees.sum()
    z['job_within_30min'] = dset.compute_range(z['emp'],30)
    z['ln_job_within_30min']=np.log( z['job_within_30min'])
    z['non_residential_sqft_mean']=b.groupby('zone_id').non_residential_sqft.mean()
    z['ln_non_residential_sqft_mean']=np.log(z['non_residential_sqft_mean'])
    z['residential_sqft_mean']=b[np.in1d(b['building_type_id'], [2,3,20,24])].groupby('zone_id').bldg_sq_ft.mean()
    z['ln_residential_sqft_mean']=np.log(z['residential_sqft_mean'])

    #z['median_value']=b.groupby('zone_id').unit_price_residential.mean()


    del z['job_within_30min']
    del z['non_residential_sqft_mean']
    del z['residential_sqft_mean']
    del z['emp']

    z['median_year_built'] = b.groupby('zone_id').year_built.median().astype('int32')
    z['ln_median_year_built']=np.log(z['median_year_built'])
    z['median_yearbuilt_post_2000'] = (b.groupby('zone_id').year_built.median()>2000).astype('int32')
    z['median_yearbuilt_pre_1970'] = (b.groupby('zone_id').year_built.median()<1970).astype('int32')
    z['zone_contains_park'] = (p[p.lu_type_id==14].groupby('zone_id').size()>0).astype('int32')

    z['zonecentroid_x2']=np.log(z['zonecentroid_x'])**2
    z['zonecentroid_y2']=np.log(z['zonecentroid_y'])**2
    z['zonecentroid_xy']=np.log(z['zonecentroid_x'])*np.log(z['zonecentroid_y'])
    z['ln_zonecentroid_x']=np.log(z['zonecentroid_x'])
    z['ln_zonecentroid_y']=np.log(z['zonecentroid_y'])
    z['zonecentroid_x3']=np.log(z['zonecentroid_x'])**3
    z['zonecentroid_y3']=np.log(z['zonecentroid_y'])**3
    z['zonecentroid_x4']=np.log(z['zonecentroid_x'])**4
    z['zonecentroid_y4']=np.log(z['zonecentroid_y'])**4


    del z['median_year_built']

    return z

#### PART 1.b.: Household characteristics using census data

def data_zone_census( zones):

    data_census=pd.read_csv('C:\Users\XGitiaux\Documents\Price UrbanSim\Data/census_zone.csv')
    #del data_census['median_value']
    data=pd.merge(zones, data_census, on='zone_id', how='inner')


    #Income using census block group dat
    data['median_income']=data['median_income'].astype(float)
    data['ln_inc']=np.log(data['median_income'])

    # Asked price (census)
    #data['median_value']=data['median_value'].apply(float)
    data['ln_price']=np.log(data['median_value'])

    # Race composition
    data['all races']=data['White alone'].apply(float)+ data['Black or African American alone'].apply(float)\
                     + data['American Indian and Alaska Native alone'].apply(float)+ data['Asian alone'].apply(float)\
                +data['Native Hawaiian and Other Pacific Islander alone'].apply(float)+ data['Some other race alone'].apply(float)\
                +data['two races or more'].apply(float)
    data['percent_white']=np.log(data['White alone']/data['all races'])
    data['percent_black']=data['Black or African American alone']/data['all races']
    data['percent_black2']=data['Black or African American alone']/data['all races']**2
    data['ln_residential_sqft_mean2']=data['ln_residential_sqft_mean']**2


    # Creating max  and min income of neighbors ( can important have implications in terms of gentrification)
    geo=pd.DataFrame(data['zonecentroid_x'])
    geo['zonecentroid_y']=data['zonecentroid_y']
    geo=np.array(geo)
    w=pysal.knnW(geo, k=10)

    n=len(geo)
    neigh_income_max=np.zeros(n)
    neigh_income_min=np.zeros(n)

    for i in range(0, n-1):
        arr=w.neighbors[i]
        zone=np.zeros(n)
        for j in arr:
         zone[j]=1

        data['neigh']=zone
        neigh_income_max[i]=data[data['neigh']==1].median_income.max()
        neigh_income_min[i]=data[data['neigh']==1].median_income.min()

    data['ln_neigh_income_max']=np.log(neigh_income_max/data['median_income'])
    data['ln_neigh_income_min']=np.log(neigh_income_min/data['median_income'])

    data=data.set_index(data['zone_id'])
    return data


###PART 2: Estimation
#### PART 2. a.: Instrument for income (highly endogenous to prices because of selection (mostly driven by financial constraint).
#### School districts are used as spatial fixed effects.

def instrument(depvar, indvar, data, instr, fixedeffect):

    # Make sure that there is no inf or nan in the data
    for varname in depvar + indvar + instr + fixedeffect:
        data=data[np.isfinite(data[varname])]

    # Generate dummies for categorical variables and remove one of them (to avoid multi-collinearity)
    inst=pd.get_dummies(data['Year_move'], prefix='YearMove')
    del inst['YearMove_2008.0']
    x=pd.get_dummies(data['school_district_id'], prefix='sdis')
    del x['sdis_8']

    # Fill the righ hand side with instruments
    collist=list(inst)
    for varname in collist :
        x[varname]=inst[varname]

    # Fill the righ hand side with exogenous variables
    for varname in indvar:
        x[varname]=data[varname]

    # Add a constant
    x['cons']=1

    # Regression (here simply OLS, but something else could be used)
    mod=sm.OLS(data[depvar], x)
    result=mod.fit()
    print result.summary()


    # Store the predicted value (that will be used on the right hand side in the second stage)
    for varname in depvar:
        data[varname+'_iv']=result.predict()

    return data



#### PART 2. b.: Second stage regression, replacing income by its predicted value from stage 1
def second_stage(depvar, indvar, data, instrumented, instr, indvariv, fixedeffect):

    # Instrumentation (first stage)
    data=instrument(instrumented,  indvariv, data, instr, fixedeffect)

    # Make sure that there is no inf or nan in the RHS/LHS variables
    for varname in depvar + indvar + fixedeffect:
        data=data[np.isfinite(data[varname])]

    #data=data[data['median_value']<400000]
    # Generate dummies for categorical variables and remove one of them (to avoid multi-collinearity)
    x=pd.get_dummies(data['school_district_id'], prefix='sdis')
    del x['sdis_8']

    # Fill the righ hand side with instruments
    for varname in indvar:
        x[varname]=data[varname]

    # Replace the instrumented variable by ita predictor from stage one
    for varname in instrumented:
        x[varname]=data[varname+'_iv']

    # Add a constant
    x['const']=1

    print x

    # Main Regression. GLM estimation using a Negative Binomial family (it seems to work better than other families)
    mod=sm.GLM(data[depvar], x, family=sm.families.Poisson())
    result=mod.fit()


    # Return Coefficient
    collist=list(x.columns.values)
    dset.store_coeff("coeff_residential",result.params.values,result.params.index)


    coeff_store_path = os.path.join(misc.data_dir(),'coeffs_res.h5')
    coeff_store = pd.HDFStore(coeff_store_path)
    coeff_store['coeffs_res'] = dset.coeffs
    coeff_store.close()

    # Predicted Prices
    data['sim_price']=result.predict()
    print result.summary()
    return data

"""
from synthicity.utils import misc
from drcog.models import dataset
dset = dataset.DRCOGDataset(os.path.join(misc.data_dir(),'drcog.h5'))
zones=data_zone(dset,dset.buildings, dset.parcels,dset.zones,dset.establishments)
data=data_zone_census(zones)
data['val_inc']=data['median_value']
ind_var=[ 'ln_job_within_30min','zonecentroid_x','zonecentroid_y','zonecentroid_x2','zonecentroid_y2', 'zonecentroid_xy','zonecentroid_x3','zonecentroid_y3','zonecentroid_x4','zonecentroid_y4',
         'median_yearbuilt_post_2000','median_yearbuilt_pre_1970','ln_non_residential_sqft_mean','ln_residential_sqft_mean', 'ln_neigh_income_max', 'ln_neigh_income_min', 'percent_black']
ind_var0=[ 'ln_job_within_30min','zonecentroid_x','zonecentroid_y','zonecentroid_x2','zonecentroid_y2', 'zonecentroid_xy','zonecentroid_x3','zonecentroid_y3','zonecentroid_y3','zonecentroid_x4','zonecentroid_y4',
         'median_yearbuilt_post_2000','median_yearbuilt_pre_1970','ln_non_residential_sqft_mean','ln_residential_sqft_mean', 'percent_black']


data=second_stage(['val_inc'], ind_var,data, ['ln_inc'], ['Year_move'],ind_var0 , ['school_district_id'])
print data[data['median_value']>400000]['sim_price'].mean()
print data[data['median_value']>400000]['median_value'].mean()
plt.plot(data['sim_price'], data['median_value'], 'ro')
plt.show()
"""