import synthicity.urbansim.interaction as interaction
import pandas as pd, numpy as np, copy
from synthicity.utils import misc
from drcog.models import transition

def simulate(dset,year,depvar = 'building_id',alternatives=None,simulation_table = 'households',
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
        new, added, new_linked = model.transition(
                hh, 2012, linked_tables={'linked': (persons, 'household_id')})
        new.loc[added,'building_id'] = -1
        dset.d['households'] = new
        dset.d['persons'] = new_linked['linked']
        # new_hhlds = {"table": "dset.households","writetotmp": "households","model": "transitionmodel","first_year": 2010,"control_totals": "dset.%s"%transition_config['control_totals_table'],
                     # "geography_field": "building_id","amount_field": "total_number_of_households"}
        # import synthicity.urbansim.transitionmodel as transitionmodel
        # transitionmodel.simulate(dset,new_hhlds,year=year,show=True,subtract=True)
        
    choosers = dset.fetch(simulation_table)
        
    if relocation_config['Enabled']:
        rate_table = dset.store[relocation_config['relocation_rates_table']].copy()
        rate_field = "probability_of_relocating"
        rate_table[rate_field] = rate_table[rate_field]*.01*relocation_config['scaling_factor']
        movers = dset.relocation_rates(choosers,rate_table,rate_field)
        choosers[depvar].ix[movers] = -1

    movers = choosers[choosers[depvar]==-1]
    print "Total new agents and movers = %d" % len(movers.index)
    empty_units = dset.buildings[(dset.buildings.residential_units>0)].residential_units.sub(choosers.groupby('building_id').size(),fill_value=0)
    empty_units = empty_units[empty_units>0].order(ascending=False)
    alternatives = alternatives.ix[np.repeat(empty_units.index,empty_units.values.astype('int'))]
    alts = alternatives
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

    new_homes = pd.Series(np.ones(len(movers.index))*-1,index=movers.index)
    for name, segment in segments:
        name_coeff = str(name)
        name = str(name)
        p=pdf['segment%s'%name].values
        mask = np.zeros(len(alts.index),dtype='bool')

        print "Assigning units to %d agents of segment %s" % (len(segment.index),name)
     
        def choose(p,mask,alternatives,segment,new_homes,minsize=None):
            p = copy.copy(p)
            p[mask] = 0 # already chosen
            try: 
              indexes = np.random.choice(len(alternatives.index),len(segment.index),replace=False,p=p/p.sum())
            except:
              print "WARNING: not enough options to fit agents, will result in unplaced agents"
              return mask,new_homes
            new_homes.ix[segment.index] = alternatives.index.values[indexes]
            mask[indexes] = 1
          
            return mask,new_homes
        mask,new_homes = choose(p,mask,alts,segment,new_homes)
        
    build_cnts = new_homes.value_counts()  #num households place in each building
    print "Assigned %d agents to %d locations with %d unplaced" % (new_homes.size,build_cnts.size,build_cnts.get(-1,0))

    table = dset.households # need to go back to the whole dataset
    table[depvar].ix[new_homes.index] = new_homes.values.astype('int32')
    dset.store_attr(output_varname,year,copy.deepcopy(table[depvar]))