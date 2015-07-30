__author__ = 'jmartinez'
import synthicity.urbansim.interaction as interaction
import pandas as pd, numpy as np, copy
from functools import partial
from synthicity.utils import misc
import multiprocessing as mp

def simulate(dset,year,depvar = 'building_id',alternatives=None,simulation_table = 'establishments',
              output_names=None,agents_groupby = ['income_3_tenure',],transition_config=None):

    output_csv, output_title, coeff_name, output_varname = output_names

    if transition_config['Enabled']:
        ct = dset.fetch(transition_config['control_totals_table'])
        ct["total_number_of_jobs"] = (ct["total_number_of_jobs"]*transition_config['scaling_factor']).astype('int32')
        new_jobs = {"table": "dset.establishments","writetotmp": "establishments","model": "transitionmodel","first_year": 2010,"control_totals": "dset.%s"%transition_config['control_totals_table'],
                    "geography_field": "building_id","amount_field": "total_number_of_jobs","size_field":"employees"}
        import synthicity.urbansim.transitionmodel as transitionmodel
        transitionmodel.simulate(dset,new_jobs,year=year,show=True)

    dset.establishments.index.name = 'establishment_id'
    choosers = dset.fetch(simulation_table)
    placed_choosers = choosers[choosers[depvar]>0]

    movers = choosers[choosers[depvar]==-1]
    print "Total new agents and movers = %d" % len(movers.index)
    dset.establishments.loc[movers.index, 'zone_id']=-1
    alternatives.building_sqft_per_job = alternatives.building_sqft_per_job.fillna(1000)
    alternatives.loc[:, 'spaces'] = alternatives.non_residential_sqft/alternatives.building_sqft_per_job  # corrected chained indexing error
    empty_units = alternatives.spaces.sub(placed_choosers.groupby('building_id').employees.sum(),fill_value=0).astype('int')
    empty_units = empty_units[empty_units>0].order(ascending=False)

    alts = alternatives.ix[empty_units.index]
    alts["supply"] = empty_units.values

    pdf = gen_probs(dset, movers, agents_groupby, alts, output_names)


    #build data structures for loop

    # split_dfs = np.array_split(movers, 4)
    #
    # groupby_list = []
    # for i in split_dfs:
    #     grp = i.groupby(['sector_id_retail_agg', 'employees'])
    #     groupby_list.append(grp)

    df_list = []


    for i in movers.groupby(['sector_id_retail_agg','employees']):
        df_list.append(i[1])

    lol = lambda lst, sz: [lst[i:i+sz] for i in range(0, len(lst), sz)]

    split_list = lol(df_list, len(df_list)/4)

    from functools import partial
    mapfunc = partial(run_job, pdf=pdf, alts=alts)

    p = mp.Pool(processes=4)
    pool_results = p.map(mapfunc, split_list)
    p.close()
    p.join()


    master_list = pool_results[0] + pool_results[1] + pool_results[2] + pool_results[3]

    b_ids = []
    e_ids = []

    for i in master_list:
        b_ids.append(i[0])
        e_ids.append(i[1])

    b_array = np.concatenate(b_ids)
    e_array = np.concatenate(e_ids)

    zone_ids = dset.buildings.loc[b_array, "zone_id"].values.astype('int32')

    dset.establishments.loc[e_array, "building_id"] = -1
    dset.establishments.loc[e_array, "zone_id"] = -1
    #
    # #add new data
    #
    dset.establishments.loc[e_array, "building_id"] = b_array
    dset.establishments.loc[e_array, "zone_id"] = zone_ids




    #mover_set = movers.groupby(['sector_id_retail_agg','employees'])
    #mover_set.apply((lambda x: pick_locations(x, alts, pdf, dset)))





def run_job(df, alts, pdf):
    #res = df.apply(pick_locations, args=(alts,pdf,))
    mapfunc = partial(pick_locations, alts=alts, pdf=pdf)
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
        alts_sample = alts #sample#alternatives
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


def pick_locations(series, alts, pdf):
    minsize = series.iloc[0].employees
    seg = "segment%d"%series.iloc[0].sector_id_retail_agg
    #choiceset = alts.loc[alts.supply >= minsize]
    choice_probs = pdf.loc[:, seg].values
    pu = pd.DataFrame(choice_probs, index=alts.index)
    pu.columns=['pro']
    pu.loc[alts.supply<minsize, 'pro']=0
    pp=np.array(pu).flatten()


    try:
        indexes = np.random.choice(len(alts.index), series.shape[0], replace=False, p=pp/pp.sum())
        selected_ids = alts.index.values[indexes]
        #zone_ids = dset.buildings.loc[selected_ids, "zone_id"].values.astype('int32')
        #alts.loc[selected_ids, "supply"] -= minsize
        alts.loc[selected_ids, "supply"] = alts.loc[selected_ids, "supply"].sub(series.employees.values)
    except:
        #selected_ids = np.tile(-1, series.shape[0])
        #zone_ids = np.tile(-1, series.shape[0])
        new_probs = pdf.loc[:, seg]
        new_probs = np.array(new_probs).flatten()
        selected_ids = np.random.choice(alts.index, series.shape[0], replace=True, p=new_probs/new_probs.sum())
        #zone_ids = dset.buildings.loc[selected_ids, "zone_id"].values.astype('int32')

    estab_index = np.array(series.index)


    return (selected_ids, estab_index)


    #clear existing data from dset.establishments

    # dset.establishments.loc[series.index, "building_id"] = -1
    # dset.establishments.loc[series.index, "zone_id"] = -1
    #
    # #add new data
    #
    # dset.establishments.loc[series.index, "building_id"] = selected_ids
    # dset.establishments.loc[series.index, "zone_id"] = zone_ids

def test_fnc(df, testParam1, testParam2):
    print type(df)
    print type(testParam1)
    print type(testParam2)
    #print type(testParam3)

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

    sim_year = 2011
    alternatives = dset.buildings[(dset.buildings.non_residential_sqft>0)]
    simulate(dset, year=sim_year,depvar = 'building_id',alternatives=alternatives,simulation_table = 'establishments',output_names = ("drcog-coeff-elcm-%s.csv","DRCOG EMPLOYMENT LOCATION CHOICE MODELS (%s)","emp_location_%s","establishment_building_ids"),
                             agents_groupby= ['sector_id_retail_agg',],transition_config = {'Enabled':True,'control_totals_table':'annual_employment_control_totals','scaling_factor':1.0})

