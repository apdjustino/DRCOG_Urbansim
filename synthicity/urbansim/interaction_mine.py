import numpy as np, pandas as pd, sys, time
import mnl, nl
import random
import pmat

GPU=0
def enable_gpu():
    global GPU
    GPU = 1
    pmat.initialize_gpu()

def estimate(data,est_params,numalts,availability=None,gpu=None):
    global GPU
    if gpu is None: gpu = GPU
    if est_params[0] == 'mnl':
        if availability:
            assert 0 # not implemented yet
        return mnl.mnl_estimate(data,est_params[1],numalts,gpu)
    elif est_params[0] == 'nl':
        return nl.nl_estimate(data,est_params[1],numalts,est_params[2],availability,gpu)

    else: assert 0

def simulate(data,coeff,numalts,gpu=GPU):
    return mnl.mnl_simulate(data,coeff,numalts,gpu)
   
# if nested logit, add the field names for the additional nesting params 
def add_fnames(fnames,est_params):  
    if est_params[0] == 'nl':
        fnames = ['mu%d' % (i+1) for i in range(est_params[2].numnests())]+fnames
    return fnames

def mnl_estimate(data,chosen,numalts,gpu=GPU):
    return mnl.mnl_estimate(data,chosen,numalts,gpu)

def mnl_simulate(data,coeff,numalts,gpu=GPU,returnprobs=0):
    return mnl.mnl_simulate(data,coeff,numalts,gpu,returnprobs)

# def mnl_interaction_dataset(choosers,alternatives,SAMPLE_SIZE,chosenalts=None):
    # numchoosers = choosers.shape[0]
    # numalts = alternatives.shape[0]
    # if SAMPLE_SIZE < numalts:
      # not_chosen_alts = alternatives.index.values[np.invert(np.in1d(alternatives.index.values,chosenalts))]
      # sample = np.random.choice(not_chosen_alts,SAMPLE_SIZE*numchoosers,replace=False) 
      # if chosenalts is not None: sample[::SAMPLE_SIZE] = chosenalts # replace with chosen alternative
      # sample = np.unique(sample)
      # if len(sample) < SAMPLE_SIZE*numchoosers:
          # num_to_add = SAMPLE_SIZE*numchoosers - len(sample)
          # to_add = range(1,(num_to_add+1))
          # sample = list(sample) + to_add
    # else:
      # assert chosenalts is None # if not sampling, must be simulating
      # assert numchoosers < 10 # we're about to do a huge join - do this with a discretized population
      # sample = np.tile(alternatives.index.values,numchoosers)
    # alts_sample = alternatives.ix[sample]
    ###pd.options.display.max_columns = 50
    # alts_sample['join_index'] = np.repeat(choosers.index,SAMPLE_SIZE)
    # try: alts_sample['join_index'] = np.repeat(choosers.index,SAMPLE_SIZE)
    # except: raise Exception("ERROR: An exception here means agents and alternatives aren't merging correctly")

    # alts_sample = pd.merge(alts_sample,choosers,left_on='join_index',right_index=True,suffixes=('','_r'))

    # chosen = np.zeros((numchoosers,SAMPLE_SIZE))
    # chosen[:,0] = 1

    # return sample, alts_sample, ('mnl',chosen)
    
def mnl_interaction_dataset(choosers,alternatives,SAMPLE_SIZE,chosenalts=None,weight_var=None):

    numchoosers = choosers.shape[0]
    numalts = alternatives.shape[0]
    if weight_var:
        sample_probs = (alternatives[weight_var]*1.0/alternatives[weight_var].sum()).values
    if SAMPLE_SIZE < numalts:
      if weight_var:
          sample = np.random.choice(alternatives.index.values,SAMPLE_SIZE*choosers.shape[0],p=sample_probs) 
      else: 
          sample = np.random.choice(alternatives.index.values,SAMPLE_SIZE*choosers.shape[0])
      if chosenalts is not None: sample[::SAMPLE_SIZE] = chosenalts # replace with chosen alternative
    else:
      print 'NOT SAMPLING'
      assert chosenalts is None # if not sampling, must be simulating
      assert numchoosers < 10 # we're about to do a huge join - do this with a discretized population
      sample = np.tile(alternatives.index.values,numchoosers)
      # print len(sample)
    # print sample
    # print choosers
    # print alternatives
    
    # for c in alternatives.columns:
        # print '"' + c + '",',
    to_drop = ['external_zone_id','area','modelarea','area_type','zonecentroid_x','zonecentroid_y','county','numtransstops','averagedailyparkingcost','intrdenshhbuffer','intrdensempbuffer','private_pk8enrollment','public_pk8enrollment','total_pk8enrollment','private_912enrollment','public_912enrollment','total_912enrollment','universityenrollment','schooldistrictzone','schooldistrictname','newdistrictname','newdistrictid','totalzonalenrollment','escort_agglogsum','persbus_agglogsum','shop_agglogsum','meal_agglogsum','socrec_agglogsum','workbasedsubtour_agglogsum']
    
    if 'tax_exempt' and 'gid' in alternatives.columns:
       to_drop = to_drop + ['improvement_value','land_area','bldg_sq_ft','tax_exempt','gid']
       
    # else:
       # print 'yoyo max'
       # to_drop = to_drop + ["gen_lu_type_id", "lu_type_id", "tax_exempt_flag", "school_district"]
       
    # newalternatives = pd.DataFrame(index=alternatives.index)
    # for colname in alternatives.columns:
      # if alternatives[colname].dtype == np.float64: newalternatives[colname] = alternatives[colname].astype('float32') 
      # elif alternatives[colname].dtype == np.int64: newalternatives[colname] = alternatives[colname].astype('int32')
      # else: newalternatives[colname] = alternatives[colname]
    # alternatives = newalternatives
    
    alternatives = alternatives.drop(to_drop,1)
    
    #alternatives = alternatives.copy()
    
    #sample = np.unique(sample)
    if chosenalts is None:
        alts_sample = alternatives
    else:
        # print 'yoyo~'
        alts_sample = alternatives.ix[sample]
    # print alts_sample
    # print len(alts_sample.index)
    # print len(choosers.index)
    # print SAMPLE_SIZE
    #alts_sample['join_index'] = np.repeat(choosers.index,SAMPLE_SIZE)
    try: alts_sample['join_index'] = np.repeat(choosers.index,SAMPLE_SIZE)
    except: raise Exception("ERROR: An exception here means agents and alternatives aren't merging correctly")

    alts_sample = pd.merge(alts_sample,choosers,left_on='join_index',right_index=True,suffixes=('','_r'))

    chosen = np.zeros((numchoosers,SAMPLE_SIZE))
    chosen[:,0] = 1

    return sample, alts_sample, ('mnl',chosen)

def mnl_choice_from_sample(sample,choices,SAMPLE_SIZE):
    return np.reshape(sample,(choices.size,SAMPLE_SIZE))[np.arange(choices.size),choices]




def nl_estimate(data,chosen,numalts,nestinfo,gpu=GPU):
    return nl.nl_estimate(data,chosen,numalts,nestinfo,gpu)

def nl_simulate(data,coeff,numalts,gpu=GPU):
    return nl.nl_simulate(data,coeff,numalts,gpu)

def nl_interaction_dataset(choosers,alternatives,SAMPLE_SIZE,nestcol,chosenalts=None,left_on='nodeid',presample=None,nestcounts=None):

    nests = alternatives[nestcol].value_counts()
    print "Alternatives in each nest\n", nests
    sample_size_per_nest = SAMPLE_SIZE/len(nests)
    print "Sample size per nest", sample_size_per_nest
    assert SAMPLE_SIZE % len(nests) == 0 # divides evenly

    if presample is None:
      sample = None
      for m in np.sort(nests.keys().values):
        nsample = np.random.choice(alternatives[alternatives[nestcol] == m].index.values,
                                                      sample_size_per_nest*choosers.shape[0]) # full sampled set
        nsample = np.reshape(nsample,(nsample.size/sample_size_per_nest,sample_size_per_nest))
        if sample == None: sample = nsample
        else: sample = np.concatenate((sample,nsample),axis=1)
    else:
      sample = presample

    if type(chosenalts) <> type(None): # means we're estimating, not simulating
        chosen = np.zeros((choosers.shape[0],SAMPLE_SIZE))
        if type(left_on) == type(''): assert left_on in choosers.columns
        chosennest = pd.merge(choosers,alternatives,left_on=left_on,right_index=True,how="left") \
                                                   .set_index(choosers.index)[nestcol] # need to maintain same index
        print "Chosen alternatives by nest\n", chosennest.value_counts()
        assert sample.shape == chosen.shape
        # this restriction should be removed in the future - for now nestids have to count from 0 to numnests-1
        assert min(nests.keys()) == 0 and max(nests.keys()) == len(nests) - 1
        sample[range(sample.shape[0]), chosennest*sample_size_per_nest] = chosenalts # replace with chosen alternative
        chosen[range(chosen.shape[0]), chosennest*sample_size_per_nest] = 1

    sample = sample.flatten().astype('object')
    alts_sample = alternatives.ix[sample]
    alts_sample['join_index'] = np.repeat(choosers.index,SAMPLE_SIZE)

    print "Merging sample (takes a while)"
    t1 = time.time()
    alts_sample = pd.merge(alts_sample,choosers,left_on='join_index',right_index=True)
    print "Done merging sample in %f" % (time.time()-t1)

    nestinfo = nl.NestInfo(nests,sample_size_per_nest,chosennest,nestcounts)

    return sample, alts_sample, ('nl', chosen, nestinfo)
