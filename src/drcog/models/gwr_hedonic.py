import numpy as np, pandas as pd, os
from synthicity.utils import misc
from drcog.models import dataset
dset = dataset.DRCOGDataset(os.path.join(misc.data_dir(),'drcog.h5'))
np.random.seed(1)
import statsmodels.api as sm
import pygwr_kernel
import random

"""
This program estimates an hedonic model for prices of residential and non-residential buildings. The benchmark method
combines:
1/ A geographically weighted regression to account for spatial non-stationarity
2/ Poisson or Negative Binonial General Linear Model to estimate a log-linear model with heteroskedastic error terms
3/ Zone employment (later-on when the data is fixed, zone average income or household characteristics)
is instrumented with average buildings characteristics in neighboring zones.

The program is organized in four parts:
1/ Create a dataset for estimation
2/ Run the first stage least squares (average zonal employment regressed on county fixed effect and
  neighboring zones characteristics). The predicted zonal employment is used as an instrument in all following regressions
3/  Run a GLM GWR methods and obtain local hedonoc parameters.
4/ Generate average coefficient for each zone

"""

## Part 1: extract variables and build dataset for estimation
def data_estimation(dset, buildings,parcels,fars,zones,establishments, bid):
    bp=buildings
    p=parcels
    f=fars
    z=zones
    e=establishments
    print bp.columns





    ## Construct additional buildings variables related to zone or parcel characteristics
    bp['zone_id']= p.zone_id[bp.parcel_id].values
    bp['dist_bus']= (p.dist_bus[bp.parcel_id].values)
    bp['ln_dist_bus']=np.log(bp['dist_bus'])
    bp['dist_rail']= (p.dist_rail[bp.parcel_id].values)
    bp['ln_dist_rail']=np.log(bp['dist_rail'])
    bp['county_id']= p.county_id[bp.parcel_id].values
    bp['centroid_x']= p.centroid_x[bp.parcel_id].values
    bp['centroid_y']= p.centroid_y[bp.parcel_id].values
    bp['year20']=pd.DataFrame((bp['year_built']>1995)*(bp['year_built']<2000)).applymap(lambda x: 1 if x else 0)
    bp['year10']=pd.DataFrame((bp['year_built']>1999)*(bp['year_built']<2006)).applymap(lambda x: 1 if x else 0)
    bp['year0']=pd.DataFrame((bp['year_built']>2005)).applymap(lambda x: 1 if x else 0)

    f['far_id']=f.index
    p['far']=f.far[p.far_id].values
    bp['far']= p.far[bp.parcel_id].values
    bp['high_land_area']=pd.DataFrame((bp['land_area']>7000)).applymap(lambda x: 1 if x else 0)
    bp['ln_nres_sqft']=np.log(bp['non_residential_sqft'])
    bp['ln_res_sqft']=np.log(bp['bldg_sq_ft'])
    bp['unit_price_res_sqft']=bp[bp['bldg_sq_ft']>0]['unit_price_residential']/bp['bldg_sq_ft']



    ### neighborhood (right now zone , later on, use a kernel) characteristics
    e['zone_id'] = bp.zone_id[e.building_id].values
    bp['building_id']=bp.index
    u=pd.DataFrame(e.groupby('building_id').sector_id.sum())
    u.columns=['sector_id']
    bp=pd.merge(bp,u, left_on='building_id', right_index=True, how='outer')



    z['zone_id']=z.index
    z['far']=p.far[z.zone_id].values
    z['sector_id_5']=bp[bp['building_type_id']==5].groupby('zone_id').sector_id.sum()
    z['sector_id_5_out']=dset.compute_range( z['sector_id_5'],15, agg=np.sum)-\
        dset.compute_range( z['sector_id_5'],5, agg=np.sum)

    z['sector_id_22']=bp[bp['building_type_id']==22].groupby('zone_id').sector_id.sum()
    z['sector_id_22_out']=dset.compute_range( z['sector_id_22'],15, agg=np.sum)-\
        dset.compute_range( z['sector_id_22'],5, agg=np.sum)

    z['sector_id_18']=bp[bp['building_type_id']==18].groupby('zone_id').sector_id.sum()
    z['sector_id_18_out']=dset.compute_range( z['sector_id_18'],15, agg=np.sum)-\
        dset.compute_range( z['sector_id_18'],5, agg=np.sum)

    z['emp_sector_mean'] = e.groupby('zone_id').employees.mean()
    z['emp_sector_agg'] = e.groupby('zone_id').employees.sum()
    z['nr_sqft_mean']=bp.groupby('zone_id').non_residential_sqft.mean()
    z['nr_price_mean']=bp.groupby('zone_id').unit_price_non_residential.mean()
    z['r_sqft_mean']=bp[(bp['building_type_id']==bid)*(bp['bldg_sq_ft']>0)].groupby('zone_id').ln_res_sqft.sum()
    z['ln_r_sqft_mean']=np.log( z['r_sqft_mean'])

    z['nr_sqft_agg']=bp.groupby('zone_id').non_residential_sqft.sum()
    z['nr_stories_mean']=bp.groupby('zone_id').stories.sum()
    z['year0_mean']=bp.groupby('zone_id').year0.mean()
    z['nr_sqft_30_10']=dset.compute_range(z['nr_sqft_mean'], 30)- z['nr_sqft_mean']

    z['r_sqft_30_10']=dset.compute_range(z[np.isfinite(z['ln_r_sqft_mean'])]['ln_r_sqft_mean'], 15, agg=np.sum)- \
            dset.compute_range(z[np.isfinite(z['ln_r_sqft_mean'])]['ln_r_sqft_mean'], 5, agg=np.sum)
    z['far_30_10']=dset.compute_range(z[np.isfinite(z['far'])]['far'], 15, agg=np.mean)- \
            dset.compute_range(z[np.isfinite(z['far'])]['far'], 5, agg=np.mean)
                      #z['ln_r_sqft_mean']
    z['stories_30_10']=dset.compute_range(z['nr_stories_mean'], 15)-\
        dset.compute_range(z['nr_stories_mean'], 5, agg=np.sum)
    z['nr_year0_30_10']=dset.compute_range(z['year0_mean'], 30)- z['year0_mean']

    # Larger Area Characteristics
    z['emp_sector_mean_30']=dset.compute_range(z['emp_sector_mean'], 30, agg=np.mean)
    z['nr_sqft_30']=dset.compute_range(z['nr_sqft_mean'], 15,agg=np.mean)


    z['r_sqft_30']=dset.compute_range(z['ln_r_sqft_mean'], 15, agg=np.sum)
    bp['emp_sector_mean_30']=z.emp_sector_mean_30[bp.zone_id].values
    bp['emp_sector_10']=z.emp_sector_mean[bp.zone_id].values
    bp['year0_10']=z.year0_mean[bp.zone_id].values
    bp['stories_10']=z.nr_stories_mean[bp.zone_id].values
    bp['nr_sqft_30_10']=z.nr_sqft_30_10[bp.zone_id].values
    bp['stories_30_10']=z.stories_30_10[bp.zone_id].values
    bp['nr_year0_30_10']=z.nr_year0_30_10[bp.zone_id].values
    bp['nr_sqft_10']=z.nr_sqft_mean[bp.zone_id].values
    bp['nr_price_10']=z.nr_price_mean[bp.zone_id].values
    bp['nr_sqft_30']=z.nr_sqft_30[bp.zone_id].values
    bp['r_sqft_30_10']=z.r_sqft_30_10[bp.zone_id].values
    bp['r_sqft_10']=z.r_sqft_mean[bp.zone_id].values

    bp['r_sqft_30']=z.r_sqft_30[bp.zone_id].values
    #bp['nr_sqft_agg']=z.nr_sqft_agg[bp.zone_id].values
    bp['ln_nr_sqft_30_10']=np.log(bp['nr_sqft_30_10'])
    bp['ln_nr_sqft_30']=np.log(bp['nr_sqft_30'])
    bp['ln_nr_sqft_10']=np.log(bp['nr_sqft_10'])
    bp['ln_r_sqft_30_10']=np.log(bp['r_sqft_30_10'])
    bp['ln_r_sqft_30']=np.log(bp['r_sqft_30'])
    bp['ln_r_sqft_10']=np.log(bp['r_sqft_10'])
    bp['ln_emp_30']=np.log(bp['emp_sector_mean_30'])
    bp['ln_emp_10']=np.log(bp['emp_sector_10'])
    bp['ln_sqft_zone']=-np.log(bp.bldg_sq_ft)+bp['r_sqft_10']
    bp['ln_sqft_out']=-np.log(bp.bldg_sq_ft)+bp['r_sqft_30']
    bp['ln_stories_zone']=-bp['stories']+bp['stories_10']
    bp['ln_sqft_out']=-np.log(bp.bldg_sq_ft)+bp['r_sqft_30_10']
    bp['ln_stories_out']=bp['stories_30_10']
    bp['sector_id_5']=z.sector_id_5[bp.zone_id].values
    bp['sector_id_5_out']=z.sector_id_5_out[bp.zone_id].values
    bp['sector_id_18']=z.sector_id_18[bp.zone_id].values
    bp['sector_id_18_out']=z.sector_id_18_out[bp.zone_id].values
    bp['sector_id_22']=z.sector_id_22[bp.zone_id].values
    bp['sector_id_22_out']=z.sector_id_22_out[bp.zone_id].values

    #bp=bp[bp['building_type_id']==bid]



    del e
    del p
    dset.d['buildings']=bp
    dset.d['zones']=z
    return dset

# Part1bis:income and age
def income_data(dset):
    df_marg=pd.read_csv('C:\Users\XGitiaux\Documents\Tableau\Census/inc_age_marg.csv', index_col='zone_id')
    df_price=pd.read_csv('C:\Users\XGitiaux\Documents\Tableau\Census/ZillowPriceZone.csv')
    df_marg['zone_id']=df_marg.index
    print df_marg[df_marg['zone_id']==1609]['zone_id']
    df_marg=pd.merge(df_price, df_marg, left_on='zone_id', right_index=True)
    df_marg.index=df_marg['zone_id']

    df_marg.fillna(0, inplace=True)
    df=pd.DataFrame(df_marg.index, index=df_marg.index)
    print df_marg.PriceZ

    df['low_income_25_44']=df_marg['Householder 25 to 44 years:Less than $10,000']+df_marg['Householder 25 to 44 years:$10,000 to $14,999']\
                     +df_marg[ 'Householder 25 to 44 years:$15,000 to $19,999']\
     +df_marg['Householder 25 to 44 years:$20,000 to $24,999']+\
      df_marg['Householder 25 to 44 years:$25,000 to $29,999']

    df['low_income_45_64']=df_marg['Householder 45 to 64 years:Less than $10,000']+\
                           df_marg['Householder 45 to 64 years:$10,000 to $14,999']\
                     +df_marg[ 'Householder 45 to 64 years:$15,000 to $19,999']\
     +df_marg['Householder 45 to 64 years:$20,000 to $24,999']+\
      df_marg['Householder 45 to 64 years:$25,000 to $29,999']

    df['low_income_65']=df_marg['Householder 65 years and over:Less than $10,000']+\
                           df_marg['Householder 65 years and over:$10,000 to $14,999']\
                     +df_marg[ 'Householder 65 years and over:$15,000 to $19,999']\
     +df_marg['Householder 65 years and over:$20,000 to $24,999']+\
      df_marg['Householder 65 years and over:$25,000 to $29,999']

    df['high_income_25_44']=df_marg['Householder 25 to 44 years:$100,000 to $124,999']+\
            df_marg['Householder 25 to 44 years:$125,000 to $149,999']+\
        df_marg['Householder 25 to 44 years:$150,000 to $199,999']+\
                            df_marg['Householder 25 to 44 years:$200,000 or more']+\
      df_marg['Householder 25 to 44 years:$60,000 to $74,999']+\
                            df_marg['Householder 25 to 44 years:$75,000 to $99,999']


    df['high_income_45_64']=df_marg['Householder 45 to 64 years:$100,000 to $124,999']+\
            df_marg['Householder 45 to 64 years:$125,000 to $149,999']+\
        df_marg['Householder 45 to 64 years:$150,000 to $199,999']+\
                            df_marg['Householder 45 to 64 years:$200,000 or more']+\
                            df_marg['Householder 45 to 64 years:$60,000 to $74,999']+\
                            df_marg['Householder 45 to 64 years:$75,000 to $99,999']


    df['high_income_65']=df_marg['Householder 65 years and over:$100,000 to $124,999']+\
            df_marg['Householder 65 years and over:$125,000 to $149,999']+\
        df_marg['Householder 65 years and over:$150,000 to $199,999']+\
                            df_marg['Householder 65 years and over:$200,000 or more']+\
     df_marg['Householder 65 years and over:$60,000 to $74,999']+\
                            df_marg['Householder 65 years and over:$75,000 to $99,999']

    # Create a csv file for Tableau
    print dset[(dset['bldg_sq_ft']>0)*(dset['building_type_id']==20)]['unit_price_res_sqft']
    dset=pd.merge(dset, df, left_on='zone_id', right_index=True, how='outer')


    df['price_bid=20']=df_marg['PriceZ']
    print dset[(dset['building_type_id']==20)*(np.isfinite(dset['unit_price_res_sqft']))*(dset['county_id']==8035)].groupby('zone_id').unit_price_res_sqft.mean()
    df['zone_id']=df.index
    df=df[np.isfinite(df['price_bid=20'])]

    df.to_csv('C:\Users\XGitiaux\Documents\Tableau\Census/UBSprice_income5.csv')

    return dset


## Part 2: Instrument for employment
def instrument(dset, instrumented, instr, ind_vars):
    print "Step: Instrument Variables"

    ### Make sure there is no nan
    z=dset
    for varname in instrumented:
        z=z[np.isfinite(z[varname])]
    for varname in instr:
        z=z[np.isfinite(z[varname])]
    for varname in ind_vars:
        z=z[np.isfinite(z[varname])]

    ### Independent variables including fixed effects
    #x=pd.get_dummies(z['county_id'])
    x=pd.DataFrame(index=z.index)
    for varname in ind_vars:
        x[varname]=z[varname]

    for varname in instr:
        x[varname]=z[varname]

    x=sm.add_constant(x,prepend=False)
    ### Dependent Variables
    y=pd.DataFrame(z[instrumented])
    print len(y)
    print len(x)
    ### Regression
    regression_results=sm.OLS(y,x).fit()
    print regression_results.summary()

    ### Return the instrument
    out=pd.DataFrame(z.index)

    for varname in instrumented:
        out[varname+"_iv"]=regression_results.predict()

    return out

## Part 3: main regression using GWR

def global_hedonic(dset,depvar, ind_vars, bid, instrumented=None, instr=None, dsetiv=None, ind_variv=None, fixed_effect=False):


    ### Instrument
    #dsetiv=dsetiv[dsetiv['building_type_id']==bid]
    for varname in instrumented:
        out=instrument(dsetiv, instrumented, instr, ind_variv)
        dset=pd.merge(dset, out, left_on='zone_id', right_index=True)



    ## Make sure there is no nan
    b=dset[dset['building_type_id']==bid]

    for varname in instrumented:
        b=b[np.isfinite(b[varname])]
        b=b[~np.isnan(b[varname])]
    for varname in ind_vars:
        b=b[np.isfinite(b[varname])]
        b=b[~np.isnan(b[varname])]
    for varname in depvar:
        b=b[np.isfinite(b[varname])]
        b=b[~np.isnan(b[varname])]

    ### Independent variables including fixed effects
    if fixed_effect==True:
        x=pd.get_dummies(b.county_id)
        x['zone_id']=b['zone_id']
        x=sm.add_constant(x,prepend=False)
    else:
        x=pd.DataFrame(b.zone_id)

    print b
    for varname in ind_vars:
        x[varname]=b[varname]

    ### Adding Instrument
    if len(instrumented)*len(instr)* len(dsetiv)*len(ind_variv)!=0:
        for varname in instrumented:
            x[varname]=b[varname+"_iv"]
    else:
        for varname in instrumented:
            x[varname]=b[varname]

    x=sm.add_constant(x,prepend=False)

    del x['zone_id']

    print b['ln_stories_out']
    ### Dependent Variables
    y=pd.DataFrame(b[depvar])



    ### Regression
    print x
    print y
    #regression_results=sm.GLM(y,x, family=sm.families.NegativeBinomial()).fit()
    regression_results=sm.OLS(y,x).fit()
    out_parm=(regression_results.params).T
    print out_parm
    print regression_results.summary()

    #### Coefficient
    out=pd.DataFrame(index=b.index)

    i=0
    for varname in list(x.columns.values):
        out[varname]=out_parm[i]
        i=i+1

    out['zone_id']=b['zone_id']
    print out
    out.to_csv('C:\urbansim\output\global_coeff_'+str(bid)+'.csv')

    return  out



def kernel_hedonic(dset,depvar, ind_vars, bid, bandwidth, instrumented=None, instr=None, dsetiv=None, ind_variv=None, fixed_effect=False):

    ### Instrument
    dsetiv=dsetiv[dsetiv['building_type_id']==bid]
    for varname in instrumented:
        out=instrument(dsetiv, instrumented, instr, ind_variv)
        dset=pd.merge(dset, out, left_on='zone_id', right_index=True)

    ## Make sure there is no nan
    b=dset[dset['building_type_id']==bid]


    for varname in instrumented:
        b=b[np.isfinite(b[varname])]
    for varname in ind_vars:
        b=b[np.isfinite(b[varname])]
    for varname in depvar:
        b=b[np.isfinite(b[varname])]



    ### Independent variables including fixed effects
    if fixed_effect==True:
        x=pd.get_dummies(b.county_id)
    else:
        x=pd.DataFrame(index=b.index)
        #x=sm.add_constant(x,prepend=False)


    for varname in ind_vars:
        x[varname]=b[varname]

    ### Adding Instrument
    if len(instrumented)*len(instr)* len(dsetiv)*len(ind_variv)!=0:
        for varname in instrumented:
            x[varname]=b[varname+"_iv"]
    else:
        for varname in instrumented:
            x[varname]=b[varname]


    ### Dependent Variables
    print b[depvar]
    y=pd.DataFrame(b[depvar])

    ### Locations
    g=pd.DataFrame(b['centroid_x'])
    g['centroid_y']=b['centroid_y']

    ### GWR
    y=np.array(y,dtype=np.float64 )
    xv=np.array(x,dtype=np.float64 )

    g=np.array(g,dtype=np.float64 )
    model = pygwr_kernel.GWR(targets=y, samples=xv, locations=g)
    print "Estimating GWR model at all data points..."
    gwr_predict,gwr_parm = model.estimate_at_target_locations(bandwidth)
    print gwr_predict


    ### Report coefficients
    out=pd.DataFrame(index=b.index)

    i=0
    for varname in list(x.columns.values):
        out[varname]=gwr_parm[:,i]
        i=i+1
    out['const']=gwr_parm[:,i]
    out['zone_id']=b['zone_id']
    out.to_csv('C:\urbansim\output\coeff_'+str(bid)+'.csv')

    return  out




def estimate_hedonic(dset,depvar, ind_vars, bid, bandwidth, instrumented=None, instr=None, dsetiv=None, ind_variv=None, fixed_effect=False):


    if bandwidth!=0:
        for i in bid:
            kernel_hedonic(dset,depvar, ind_vars, i, bandwidth, instrumented, instr, dsetiv, ind_variv, fixed_effect)

    else:
        for i in bid:
            dset[depvar]=np.log(dset[depvar])
            global_hedonic(dset, depvar, ind_vars, i, instrumented, instr, dsetiv, ind_variv, fixed_effect)

"""
ind_vars=['stories',  'ln_nres_sqft',  'high_land_area', 'year0', 'year10','year20', 'ln_dist_bus', 'far', 'ln_emp_30',
           'ln_nr_sqft_10', 'ln_nr_sqft_30', 'ln_emp_10']
ind_vars2=[ 'ln_nr_sqft_10', 'ln_nr_sqft_30', 'ln_emp_30',  ]
dset=data_estimation(dset,dset.buildings, dset.parcels, dset.fars, dset.zones, dset.establishments, 20)

b=dset.buildings
b=income_data(b)
b=b[b['unit_price_res_sqft']>0]

#Randomly hold back 25 % of the sample
#b=b.ix[random.sample(b.index, int(len(b)))]

z=dset.zones
b.drop_duplicates(['stories',  'ln_res_sqft',  'high_land_area', 'year0', 'year10','year20', 'ln_dist_bus', 'far', 'ln_emp_30',
           'ln_nr_sqft_10', 'ln_nr_sqft_30', 'ln_emp_10'], inplace=True)
#b=b[b['unit_price_non_residential']<10000]


ind_vars=['stories',  'ln_res_sqft',  'high_land_area', 'year0', 'year10','year20', 'ln_dist_bus','ln_sqft_zone',
           'far',  'ln_sqft_out','ln_stories_zone', 'ln_stories_out', 'ln_emp_30','ln_emp_10','low_income_25_44', 'low_income_45_64',
           'low_income_65', 'high_income_25_44', 'high_income_65', 'high_income_45_64']
ind_vars2=['stories',  'ln_res_sqft',  'high_land_area', 'year0', 'year10','year20', 'ln_dist_bus', 'ln_sqft_zone',
          'ln_sqft_out', 'far', 'ln_stories_zone', 'ln_stories_out','low_income_25_44','low_income_25_44', 'low_income_45_64',
           'low_income_65', 'high_income_25_44', 'high_income_65', 'high_income_45_64']


out=estimate_hedonic(b,['unit_price_residential'],ind_vars
       , [20],0, instrumented=[], instr=['sector_id_5', 'sector_id_5_out','sector_id_18', 'sector_id_18_out',
                                                    'sector_id_22', 'sector_id_22_out', ]
       , dsetiv=b, ind_variv=ind_vars2, fixed_effect=False)
"""
## Part 4: Create average hedonic coefficients for each zone. These coefficients will be used in simulations to compute
## average zone price. If there is no building in the zone, we use county level average.

def estimate_zone(dset, ind_vars, bid):

    listc=[]
    ind_vars=ind_vars+['const']
    for b in bid:
        df=pd.read_csv('C:/urbansim/output/global_coeff_'+str(b)+'.csv')

        ## Need county_id
        p=pd.DataFrame(dset.parcels.zone_id)
        p['county_id']=dset.parcels.county_id


        list=[]


        for x in ind_vars:
            u=pd.DataFrame(df.groupby('zone_id')[x].mean())
            u.columns=[x]


            v=pd.merge(p,u,left_on='zone_id', right_index=True, how='outer')

            ## Use county average if no zone average
            wc=pd.DataFrame(v[~np.isnan(v[x])].groupby('county_id')[x].mean())
            wc.columns=['county_'+x]
            v=pd.merge(v,wc,left_on='county_id', right_index=True)
            v[x].fillna(v['county_'+x], inplace=True)

            w=pd.DataFrame(v.groupby('zone_id')[x].mean())
            w.columns=[x]
            list.append(w)


        coeff=pd.concat(list, axis=1)
        coeff['zone_id']=coeff.index
        coeff['bid']=b


        listc.append(coeff)

    coeff_dset=pd.concat(listc)


    return coeff_dset
