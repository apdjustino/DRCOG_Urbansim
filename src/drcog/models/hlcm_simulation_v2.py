__author__ = 'jmartinez'
import synthicity.urbansim.interaction as interaction
import pandas as pd, numpy as np, copy, os
from synthicity.utils import misc
from sqlalchemy import *
import multiprocessing as mp
from drcog.models import transition
from functools import partial

def simulate(dset,year,depvar = 'building_id',alternatives=None,simulation_table = None,
              output_names=None,agents_groupby = ['income_3_tenure',],transition_config=None,relocation_config=None):


    output_csv, output_title, coeff_name, output_varname = output_names

    if transition_config['Enabled']:
        ct = dset.fetch(transition_config['control_totals_table'])
        if 'persons' in ct.columns:
            del ct['persons']
        ct["total_number_of_households"] = (ct["total_number_of_households"]*transition_config['scaling_factor']).astype('int32')
        hh = dset.fetch('households')
        persons = dset.fetch('persons')
        tran = transition.TabularTotalsTransition(ct, 'total_number_of_households')
        model = transition.TransitionModel(tran)
        #import pdb; pdb.set_trace()
        new, added, new_linked = model.transition(
                hh, year, linked_tables={'linked': (persons, 'household_id')})
        new.loc[added,'building_id'] = -1
        dset.d['households'] = new
        dset.d['persons'] = new_linked['linked']




    dset.households.index.name = 'household_id'
    #choosers = dset.fetch(simulation_table)
    choosers = dset.households

    if relocation_config['Enabled']:
        rate_table = dset.store[relocation_config['relocation_rates_table']].copy()
        rate_field = "probability_of_relocating"
        rate_table[rate_field] = rate_table[rate_field]*.01*relocation_config['scaling_factor']
        movers = dset.relocation_rates(choosers,rate_table,rate_field)
        choosers[depvar].ix[movers] = -1

    movers_all = choosers[choosers[depvar]==-1]

    #distribute county_ids based on demography projections

    county_growth_share = pd.read_csv(os.path.join(misc.data_dir(),'county_growth_share.csv'), index_col=0 )
    counties = county_growth_share.columns.values
    current_growth_shares = county_growth_share.loc[year].values
    movers_counties = np.random.choice(counties, movers_all.shape[0], replace=True, p=current_growth_shares)

    movers_all['county_id'] = movers_counties
    empty_units = dset.buildings[(dset.buildings.residential_units>0)].residential_units.sub(choosers.groupby('building_id').size(),fill_value=0)
    empty_units = empty_units[empty_units>0].order(ascending=False)
    #alts = alternatives.ix[np.repeat(empty_units.index.values,empty_units.values.astype('int'))]
    alts = alternatives.loc[empty_units.index]

    #create alternatives subset with mortage info
    r = .05/12
    n = 360

    alts.loc[:, 'payment'] = alts.unit_price_residential*((r*(1+r)**n)/((1+r)**n-1))

    try:
        subset_alts = alts[['unit_price_residential', 'county_id']]
    except:
        subset_alts = alts[['unit_price_residential', 'county_id_y']]
        subset_alts.rename(columns={'county_id_y':'county_id'}, inplace=True)
        alts.rename(columns={'county_id_y':'county_id'}, inplace=True)

    #generate probabilities
    pdf = gen_probs(dset, movers_all, agents_groupby, alts, output_names)
    income_limits = {1:60000/12, 2:120000/12, 3:dset.households.income.max()/12, 4:40000/12, 5:dset.households.income.max()/12}
    # mapfunc = partial(pick_locations, pdf=pdf, alts=alts, income_limits=income_limits)
    #
    # map(mapfunc, movers_all.groupby(['income_3_tenure', 'county_id']))
    #
    #
    # print 'Done!'

    df_list = []

    try:
        grp_movers = movers_all.groupby(['income_3_tenure', 'county_id'])
    except:
        movers_all.rename(columns={'county_id_y':'county_id'}, inplace=True)
        grp_movers = movers_all.groupby(['income_3_tenure', 'county_id'])


    for i in grp_movers:
        df_list.append(i[1])

    #function that splits lists into equal sizes

    lol = lambda lst, sz: [lst[i:i+sz] for i in range(0, len(lst), sz)]

    split_list = lol(df_list, len(df_list)/4)

    # boundaries for income categories


    mapfunc = partial(run_job, pdf=pdf, alts=alts, income_limits=income_limits)

    p = mp.Pool(processes=4)
    pool_results = p.map(mapfunc, split_list)
    p.close()
    p.join()


    master_list = pool_results[0] + pool_results[1] + pool_results[2] + pool_results[3]

    b_ids = []
    hh_ids = []

    for i in master_list:
        b_ids.append(i[0])
        hh_ids.append(i[1])


    b_array = np.concatenate(b_ids)
    hh_array = np.concatenate(hh_ids)

    zone_ids = dset.buildings.loc[b_array, "zone_id"].values.astype('int32')

    dset.households.loc[hh_array, "building_id"] = -1
    dset.households.loc[hh_array, "zone_id"] = -1
    #
    # #add new data
    #
    dset.households.loc[hh_array, "building_id"] = b_array
    dset.households.loc[hh_array, "zone_id"] = zone_ids




def run_job(df, alts, pdf, income_limits):
    #res = df.apply(pick_locations, args=(alts,pdf,))
    mapfunc = partial(pick_locations, alts=alts, pdf=pdf, income_limits=income_limits)
    res = map(mapfunc, df)
    return res

def gen_probs(dset, movers, agents_groupby, alts, output_names):
    output_csv, output_title, coeff_name, output_varname = output_names

    pdf = pd.DataFrame(index=alts.index)
    segments = movers.groupby(agents_groupby)
    for name, segment in segments:
        segment = segment.head(1)
        name = str(name)
        tmp_outcsv, tmp_outtitle, tmp_coeffname = output_csv%name, output_title%name, coeff_name%name
        ind_vars = dset.coeffs[(tmp_coeffname, 'fnames')][np.invert(dset.coeffs[(tmp_coeffname, 'fnames')].isnull().values)].values.tolist()
        SAMPLE_SIZE = alts.index.size
        numchoosers = segment.shape[0]
        numalts = alts.shape[0]
        sample = np.tile(alts.index.values,numchoosers)
        alts_sample = alts
        alts_sample['join_index'] = np.repeat(segment.index.values,SAMPLE_SIZE)
        alts_sample = pd.merge(alts_sample,segment,left_on='join_index',right_index=True,suffixes=('','_r'))
        chosen = np.zeros((numchoosers,SAMPLE_SIZE))
        chosen[:,0] = 1
        sample, alternative_sample, est_params = sample, alts_sample, ('mnl',chosen)
        ##Interaction variables
        interaction_vars = [(var, var.split('_x_')) for var in ind_vars if '_x_' in var]
        for ivar in interaction_vars:
            if ivar[1][0].endswith('gt'):
                alternative_sample[ivar[0]] = ((alternative_sample[ivar[1][0]])>alternative_sample[ivar[1][1]]).astype('int32')
            if ivar[1][0].endswith('lt'):
                alternative_sample[ivar[0]] = ((alternative_sample[ivar[1][0]])<alternative_sample[ivar[1][1]]).astype('int32')
            else:
                alternative_sample[ivar[0]] = ((alternative_sample[ivar[1][0]])*alternative_sample[ivar[1][1]])

        est_data = pd.DataFrame(index=alternative_sample.index)
        for varname in ind_vars:
            est_data[varname] = alternative_sample[varname]
        est_data = est_data.fillna(0)
        data = est_data
        data = data.as_matrix()
        coeff = dset.load_coeff(tmp_coeffname)
        probs = interaction.mnl_simulate(data,coeff,numalts=SAMPLE_SIZE,returnprobs=1)
        pdf['segment%s'%name] = pd.Series(probs.flatten(),index=alts.index)
    return pdf


def pick_locations(series, alts, pdf, income_limits):

    segment_income = series.iloc[0].income_3_tenure
    seg = "segment%d"%segment_income
    county_id = int(series.iloc[0].county_id)
    segment_income_val = income_limits[segment_income]
    bool_county = (alts.county_id != county_id)
    bool_income = (alts.payment / segment_income_val > 0.33)
    bool_available = (alts.residential_units <= 0)


    choice_probs = pdf.loc[:, seg].values
    pu = pd.DataFrame(choice_probs, index=alts.index)
    pu.columns=['pro']
    pu.loc[(bool_county) & (bool_income) & (bool_available) , 'pro']=0
    pp=np.array(pu).flatten()

    try:
        indexes = np.random.choice(len(alts.index), series.shape[0], replace=False, p=pp/pp.sum())
        selected_ids = alts.index.values[indexes]
        alts.loc[selected_ids, "residential_units"] -= 1

    except:
        #selected_ids = np.tile(-1, series.shape[0])
        #zone_ids = np.tile(-1, series.shape[0])
        new_probs = pdf.loc[:, seg]
        new_probs = np.array(new_probs).flatten()
        selected_ids = np.random.choice(alts.index, series.shape[0], replace=True, p=new_probs/new_probs.sum())
        #zone_ids = dset.buildings.loc[selected_ids, "zone_id"].values.astype('int32')

    hh_index = np.array(series.index)

    return (selected_ids, hh_index)


if __name__ == '__main__':
    from drcog.models import dataset
    from drcog.variables import variable_library
    import os
    import cProfile
    dset = dataset.DRCOGDataset(os.path.join(misc.data_dir(),'drcog.h5'))

    #Load estimated coefficients
    coeff_store = pd.HDFStore(os.path.join(misc.data_dir(),'coeffs.h5'))
    dset.coeffs = coeff_store.coeffs.copy()
    coeff_store.close()

    coeff_store = pd.HDFStore(os.path.join(misc.data_dir(),'coeffs_res.h5'))
    dset.coeffs_res = coeff_store.coeffs_res.copy()
    coeff_store.close()

    variable_library.calculate_variables(dset)
    alternatives = dset.buildings[(dset.buildings.residential_units>0)]
    sim_year = 2011

    fnc = "simulate(dset, year=sim_year,depvar = 'building_id',alternatives=alternatives,simulation_table = 'households',output_names = ('drcog-coeff-hlcm-%s.csv','DRCOG HOUSEHOLD LOCATION CHOICE MODELS (%s)','hh_location_%s','household_building_ids')," +\
                                         "agents_groupby= ['income_3_tenure',],transition_config = {'Enabled':True,'control_totals_table':'annual_household_control_totals','scaling_factor':1.0}," +\
                                         "relocation_config = {'Enabled':True,'relocation_rates_table':'annual_household_relocation_rates','scaling_factor':1.0},)"

    cProfile.run(fnc, 'c:/users/jmartinez/documents/projects/urbansim/cprofile/hlcm')