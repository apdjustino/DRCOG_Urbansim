__author__ = 'jmartinez'
import synthicity.urbansim.interaction as interaction
import pandas as pd, numpy as np, copy, os
from synthicity.utils import misc
from sqlalchemy import *
import multiprocessing as mp


from drcog.models import transition



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

    #calculate mortgage payment values

    temp_count = 0

    buildings = alternatives
    out_table = pd.DataFrame(columns=dset.households.columns)
    homeless = pd.DataFrame(columns=dset.households.columns)
    r = .05/12
    n = 360
    buildings['est_mortgage_payment']=buildings.unit_price_residential*((r*(1+r)**n)/((1+r)**n-1))

    dset.households.index.name = 'household_id'
    choosers = dset.fetch(simulation_table)

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
    alts = alternatives.ix[np.repeat(empty_units.index.values,empty_units.values.astype('int'))]

    #create alternatives subset with mortage info
    r = .05/12
    n = 360

    try:
        subset_alts = alts[['unit_price_residential', 'county_id']]
    except:
        subset_alts = alts[['unit_price_residential', 'county_id_y']]
        subset_alts.rename(columns={'county_id_y':'county_id'}, inplace=True)

    subset_alts['payment'] = alts.unit_price_residential*((r*(1+r)**n)/((1+r)**n-1))


    #generate probabilities
    pdf = gen_probs(dset, movers_all, agents_groupby, alts, output_names)

    #build data structures for loop


    #income_3_tenure limits
    income_limits = {1:60000/12, 2:120000/12, 3:dset.households.income.max()/12, 4:40000/12, 5:dset.households.income.max()/12}

    bool_price1 = (subset_alts.payment / income_limits[1]) <= 0.33
    bool_price2 = (subset_alts.payment / income_limits[2]) <= 0.33
    bool_price3 = (subset_alts.payment / income_limits[3]) <= 0.33
    bool_price4 = (subset_alts.payment / income_limits[4]) <= 0.33
    bool_price5 = (subset_alts.payment / income_limits[5]) <= 0.33
    d = {}


    for county in counties:
        data_list = []
        bool_counties = subset_alts.county_id == int(county)
        ids1 = subset_alts.loc[(bool_counties) & (bool_price1)].index.tolist()
        ids2 = subset_alts.loc[(bool_counties) & (bool_price2)].index.tolist()
        ids3 = subset_alts.loc[(bool_counties) & (bool_price3)].index.tolist()
        ids4 = subset_alts.loc[(bool_counties) & (bool_price4)].index.tolist()
        ids5 = subset_alts.loc[(bool_counties) & (bool_price5)].index.tolist()
                ##generate lists of probabilities
        prob1 = pdf.loc[set(ids1), 'segment1'].tolist()
        prob2 = pdf.loc[set(ids2), 'segment2'].tolist()
        prob3 = pdf.loc[set(ids3), 'segment3'].tolist()
        prob4 = pdf.loc[set(ids4), 'segment4'].tolist()
        prob5 = pdf.loc[set(ids5), 'segment5'].tolist()

        data_list.append((ids1, prob1))
        data_list.append((ids2, prob2))
        data_list.append((ids3, prob3))
        data_list.append((ids4, prob4))
        data_list.append((ids5, prob5))

        d[int(county)] = data_list





    #call placing method

    m_loop = movers_all[['income_3_tenure','county_id','building_id']]
    #m_loop = m_loop.head(5000)
    out_list = []

    from functools import partial
    mapfunc = partial(apply_func, d=d, out=out_list)
    p = mp.Pool(processes=4)
    split_dfs = np.array_split(m_loop, 4)
    pool_results = p.map(mapfunc, split_dfs)
    p.close()
    p.join()

    #m_loop.apply(place_households, axis=1, args=(d,out_list))
    master_list = pool_results[0] + pool_results[1] + pool_results[2] + pool_results[3]

    building_ids = [i[0] for i in master_list]
    household_id = [i[1] for i in master_list]

    result_frame = pd.DataFrame(columns=['household_id', 'building_id'])
    result_frame['household_id'] = household_id
    result_frame['building_id'] = building_ids
    #
    dset.households.loc[result_frame.household_id, 'building_id'] = result_frame['building_id'].values
    #
    #result_frame.to_csv('c:/users/jmartinez/documents/test_results.csv')

    #print out_list

    dset.households.loc[result_frame.household_id]






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


def apply_func(df, d, out):
    res = df.apply(place_households, axis=1, args=(d,out))
    return out
def place_households(series,d,output):
    hh_id = series.name
    income_3_tenure = int(series.income_3_tenure) - 1
    county_id = int(series.county_id)
    building_id = series.building_id

    id_list = d[county_id][income_3_tenure][0]
    prob_list = d[county_id][income_3_tenure][1]
    prob_list = prob_list[0:len(id_list)]

    if len(id_list) > 0:
        chosen_index = np.random.choice(id_list, 1, replace=False, p=np.asarray(prob_list)/np.asarray(prob_list).sum())
        d[county_id][income_3_tenure][0].remove(chosen_index)
        output.append((chosen_index[0], hh_id))
    else:
        backstop_ids = d[county_id][2][0]
        backstop_prob_list = d[county_id][2][1]
        try:
            chosen_index = np.random.choice(backstop_ids, 1, replace=False)
            d[county_id][2][0].remove(chosen_index)
        except:
            chosen_index = [-1]

        output.append((chosen_index[0], hh_id))

        #chosen_index = -1
        #output.append((chosen_index, hh_id))
    #try:



    #except:
    #    chosen_index = -1
    #    output.append((chosen_index, hh_id))

    #output.loc[hh_id, 'building_id'] = chosen_index
















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