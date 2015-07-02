import synthicity.urbansim.interaction as interaction
import pandas as pd, numpy as np, copy
from synthicity.utils import misc
import os

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
    county_growth_share = pd.read_csv(os.path.join(misc.data_dir(),'county_growth_share.csv'), index_col=0 )
    counties = county_growth_share.columns.values
    current_growth_shares = county_growth_share.loc[year].values
    movers_counties = np.random.choice(counties, movers_all.shape[0], replace=True, p=current_growth_shares)

    movers_all['county_id'] = movers_counties


    income_segment = movers_all.groupby('income_grp')['upper_income_grp_val','lower_income_grp_val'].agg([np.mean, np.size])
    # get county growth control data and merge with income_segements

    income_segment['county'] = county_growth_share.loc[year].index.values[0]
    income_segment['growth_share'] = county_growth_share.loc[year][0]
    copy_df = income_segment.copy()
    for i in county_growth_share.loc[year][1:].iteritems():

        copy_df['county'] = i[0]
        copy_df['growth_share'] = i[1]
        income_segment = pd.concat([income_segment, copy_df])

    income_segment = income_segment.set_index(['county', income_segment.index])

    print "Total new agents and movers = %d" % len(movers_all.index)



    for seg in income_segment.iterrows():


        movers = movers_all[(movers_all['income']<= seg[1][0]) & (movers_all['income']>= seg[1][2])]
        print 'County: %s. Placing %d households in the income range (%d, %d)' % (seg[0][0],seg[1][1],seg[1][2], seg[1][0])

        empty_units = buildings.residential_units.sub(choosers[choosers['building_id']!=-1].groupby('building_id').size(),fill_value=0)
        empty_units = empty_units[empty_units>0].order(ascending=False)
        print 'number of empty units is %d' %empty_units.sum()
        alternatives = buildings.ix[np.repeat(empty_units.index.values,empty_units.values.astype('int'))]
        alternatives = alternatives[alternatives.county_id == int(seg[0][0])]

        if((seg[1][2]/12) <= 0):
            alts = alternatives[alternatives['unit_price_residential'] < 186281]
        elif((seg[1][2]/12) >= 55000):
            alts = alternatives[alternatives['unit_price_residential'] > 1583400]
        else:
            alts = alternatives[alternatives['est_mortgage_payment'] / (seg[1][2]/12) <= 0.33]
        if(alts.shape[0] == 0):
            homeless = pd.concat([choosers, homeless])
            print 'Could not place %d households due to income restrictions' % seg[1][1]
            continue




        pdf = pd.DataFrame(index=alts.index)

        segments = movers.groupby(agents_groupby)

        ##simulation
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

        new_homes = pd.Series(np.ones(len(movers.index))*-1,index=movers.index)
        for name, segment in segments:
            name_coeff = str(name)
            name = str(name)
            p=pdf['segment%s'%name]
            mask = np.zeros(len(alts.index),dtype='bool')
            mask = pd.Series(mask, index=alts.index)

            print "Assigning units to %d agents of segment %s" % (len(segment.index),name)

            def choose(p,mask,alternatives,segment,new_homes,minsize=None):
                p = copy.copy(p)
                p.loc[mask[mask==True].index] = 0 # already chosen
                try:
                    indexes = np.random.choice(alternatives.index.values,len(segment.index),replace=False,p=p.values/p.values.sum())
                except:
                    print "WARNING: not enough options to fit agents, will result in unplaced agents"
                    indexes = np.random.choice(alternatives.index.values,len(alternatives.index.values),replace=False,p=p.values/p.values.sum())

                    if(new_homes.ix[segment[segment.tenure==2].index.values[:len(alternatives.index.values)]].shape[0] != 0):
                        new_homes.ix[segment[segment.tenure==2].index.values[:len(alternatives.index.values)]] = -2
                    else:
                        new_homes.ix[segment.index.values[:len(alternatives.index.values)]] = alternatives.index.values

                    mask.loc[indexes] = True
                    return mask,new_homes

                new_homes.ix[segment.index] = alternatives.loc[indexes].index.values[:len(new_homes.ix[segment.index])]
                mask.loc[indexes] = True

                return mask,new_homes
            mask,new_homes = choose(p,mask,alts,segment,new_homes)

        build_cnts = new_homes.value_counts()  #num households place in each building
        print "Assigned %d agents to %d locations with %d unplaced" % (new_homes.size,build_cnts.size,build_cnts.get(-1,0))

        table = dset.households # need to go back to the whole dataset
        table[depvar].ix[new_homes.index] = new_homes.values.astype('int32')
        #table.to_sql('tmp_out', engine, if_exists='append')
        table = table.ix[new_homes.index]
        out_table = pd.concat([table, out_table])
        choosers.loc[table.index] = table
        #dset.store_attr(output_varname,year,copy.deepcopy(table[depvar]))
        # old_building_count = buildings.shape[0]
        # buildings = buildings.drop(new_homes.index)
        # new_building_count = buildings.shape[0]
        # print '%d units were filled' %(new_building_count - old_building_count)
        #buildings = buildings.drop(new_homes)
        #temp_count += 1
        if(temp_count > 50):
            break
    #out_table.to_csv('C:/users/jmartinez/documents/households_simulation_test.csv')
    dset.households.loc[out_table.index] = out_table
    #homeless.to_csv('C:/users/jmartinez/documents/homeless.csv')

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