import numpy as np, pandas as pd, os
from synthicity.utils import misc
from drcog.models import dataset
from drcog.variables import variable_library
from sqlalchemy import *
dset = dataset.DRCOGDataset(os.path.join(misc.data_dir(),'drcog.h5'))
np.random.seed(1)

def simulate(dset,year,depvar = 'building_id',alternatives=None,simulation_table = None,
              output_names=None,agents_groupby = ['income_3_tenure',],transition_config=None,relocation_config=None):

    import synthicity.urbansim.interaction as interaction
    import pandas as pd, numpy as np, copy

    from synthicity.utils import misc
    from drcog.models import transition


    temp_count = 0
    output_csv, output_title, coeff_name, output_varname = output_names
    buildings = alternatives
    income_segment = dset.households.groupby('income').size()
    out_table = pd.DataFrame(columns=dset.households.columns)
    homeless = pd.DataFrame(columns=dset.households.columns)
    r = .05/12
    n = 360
    buildings['est_mortgage_payment']=buildings.unit_price_residential*((r*(1+r)**n)/((1+r)**n-1))

    for seg in income_segment.iteritems():


        choosers = simulation_table[simulation_table['income']== seg[0]]
        print 'Placing %d households with an income of % d' % (seg[1],seg[0])

        empty_units = buildings.residential_units.sub(simulation_table[simulation_table['building_id']!=-1].groupby('building_id').size(),fill_value=0)
        empty_units = empty_units[empty_units>0].order(ascending=False)
        print 'number of empty units is %d' %empty_units.sum()
        alternatives = buildings.ix[np.repeat(empty_units.index,empty_units.values.astype('int'))]


        if((seg[0]/12) <= 0):
            alts = alternatives[alternatives['unit_price_residential'] < 186281]
        elif((seg[0]/12) >= 55000):
            alts = alternatives[alternatives['unit_price_residential'] > 1583400]
        else:
            alts = alternatives[alternatives['est_mortgage_payment'] / (seg[0]/12) <= 0.33]
        if(alts.shape[0] == 0):
            homeless = pd.concat([choosers, homeless])
            print 'Could not place %d households due to income restrictions' % seg[1]
            continue




        pdf = pd.DataFrame(index=alts.index)

        segments = choosers.groupby(agents_groupby)

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
            alts_sample['join_index'] = np.repeat(segment.index,SAMPLE_SIZE)
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

        new_homes = pd.Series(np.ones(len(choosers.index))*-1,index=choosers.index)
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

        table = simulation_table # need to go back to the whole dataset
        table[depvar].ix[new_homes.index] = new_homes.values.astype('int32')
        #table.to_sql('tmp_out', engine, if_exists='append')
        table = table.ix[new_homes.index]
        out_table = pd.concat([table, out_table])
        simulation_table.loc[table.index] = table
        #dset.store_attr(output_varname,year,copy.deepcopy(table[depvar]))
        # old_building_count = buildings.shape[0]
        # buildings = buildings.drop(new_homes.index)
        # new_building_count = buildings.shape[0]
        # print '%d units were filled' %(new_building_count - old_building_count)
        #buildings = buildings.drop(new_homes)
        #temp_count += 1
        if(temp_count > 50):
            break
    out_table.to_csv('C:/users/jmartinez/documents/households_new_location.csv')
    #homeless.to_csv('C:/users/jmartinez/documents/homeless.csv')

dset = dataset.DRCOGDataset(os.path.join(misc.data_dir(),'drcog.h5'))
coeff_store = pd.HDFStore(os.path.join(misc.data_dir(),'coeffs.h5'))
dset.coeffs = coeff_store.coeffs.copy()
coeff_store.close()
orig_building_cols = dset.buildings.columns
column_list = []
for i in orig_building_cols:
    column_list.append(i)



for i in range(1,5):
    for j in dset.coeffs[('hh_location_' + str(i), 'fnames')].values:
        column_list.append(j)
# column_list.append(dset.coeffs[('hh_location_1', 'fnames')].values)
# column_list.append(dset.coeffs[('hh_location_2', 'fnames')].values)
# column_list.append(dset.coeffs[('hh_location_3', 'fnames')].values)
# column_list.append(dset.coeffs[('hh_location_4', 'fnames')].values)
# column_list.append(dset.coeffs[('hh_location_5', 'fnames')].values)

column_set = set(column_list)
column_list = []
for i in column_set:
    if(type(i) is str):
        column_list.append(i)

column_list.append('percent_hh_with_child')
column_list.append('avg_unit_price_zone')
column_list.append('average_resunit_size')

variable_library.calculate_variables(dset)

hh = dset.households
hh['building_id'] = -1



alternatives = dset.buildings.ix[:,column_list]




alternatives = alternatives[(alternatives.residential_units>0)]

simulate(dset, year=2011,depvar = 'building_id',alternatives=alternatives,simulation_table = hh,output_names = ("drcog-coeff-hlcm-%s.csv","DRCOG HOUSEHOLD LOCATION CHOICE MODELS (%s)","hh_location_%s","household_building_ids"),
                         agents_groupby= ['tenure',],transition_config = {'Enabled':True,'control_totals_table':'annual_household_control_totals','scaling_factor':1.0},
                         relocation_config = {'Enabled':True,'relocation_rates_table':'annual_household_relocation_rates','scaling_factor':1.0},)
