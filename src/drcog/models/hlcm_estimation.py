import synthicity.urbansim.interaction as interaction
import pandas as pd
from synthicity.utils import misc

def estimate (dset,indvars,depvar = 'building_id',alternatives=None,SAMPLE_SIZE=100,max_segment_size = 1200,estimation_table = 'households_for_estimation',
              output_names=None,agents_groupby = ['income_3_tenure',]):
    ##HCLM ESTIMATION
    output_csv, output_title, coeff_name, output_varname = output_names
    choosers = dset.fetch(estimation_table)

    segments = choosers.groupby(agents_groupby)
    num_segments = len(segments.size().index)
    if num_segments != len(indvars):
        print "ERROR: number of segments does not match number of sets of independent variable"
    indvar_dict = dict(zip(segments.size().index.values,range(num_segments)))
    alts = alternatives
    for name, segment in segments:
        ind_vars = indvars[indvar_dict[name]]
        name = str(name)
        tmp_outcsv, tmp_outtitle, tmp_coeffname = output_csv%name, output_title%name, coeff_name%name
        if len(segment[depvar]) > max_segment_size: #reduce size of segment if too big so things don't bog down
            segment = segment.ix[np.random.choice(segment.index, max_segment_size,replace=False)]
        #,weight_var='residential_units')
        sample, alternative_sample, est_params = interaction.mnl_interaction_dataset(segment,alts,SAMPLE_SIZE,chosenalts=segment[depvar])
        ##Interaction variables
        interaction_vars = [(var, var.split('_x_')) for var in ind_vars if '_x_' in var]
        for ivar in interaction_vars:
            alternative_sample[ivar[0]] = ((alternative_sample[ivar[1][0]])*alternative_sample[ivar[1][1]])

        print "Estimating parameters for segment = %s, size = %d" % (name, len(segment.index)) 
        if len(segment.index) > 50:
            est_data = pd.DataFrame(index=alternative_sample.index)
            for varname in ind_vars:
                est_data[varname] = alternative_sample[varname]
            est_data = est_data.fillna(0)
            data = est_data.as_matrix()
            try:
                fit, results = interaction.estimate(data, est_params, SAMPLE_SIZE)
                fnames = interaction.add_fnames(ind_vars,est_params)
                print misc.resultstotable(fnames,results)
                misc.resultstocsv(fit,fnames,results,tmp_outcsv,tblname=tmp_outtitle)
                coefficients = zip(*results)[0]+(.0001,.0001,.0001,.0001,.0001,.0001,.0001,.0001,.0001,.0001,.0001,)
                varnames = fnames+['county8001','county8005','county8013','county8014','county8019','county8031','county8035','county8039','county8047','county8059','county8123']
                dset.store_coeff(tmp_coeffname,coefficients,varnames)
            except:
                print 'SINGULAR MATRIX OR OTHER DATA/ESTIMATION PROBLEM'
        else:
            print 'SAMPLE SIZE TOO SMALL'