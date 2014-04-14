import statsmodels.api as sm
import pandas as pd, numpy as np
from synthicity.utils import misc

def estimate (dset,indvars,depvar_name = 'price',max_segment_size = 15000,estimation_table = 'buildings',output_names=None,agents_groupby = ['building_type_id',]):

    output_csv, output_title, coeff_name, output_varname = output_names
    choosers = dset.fetch(estimation_table)
    
    segments = choosers.groupby(agents_groupby)
    num_segments = len(segments.size().index)
    if num_segments != len(indvars):
        print "ERROR: number of segments does not match number of sets of independent variable"
        
    indvar_dict = dict(zip(segments.size().index.values,range(num_segments)))
    
    for name, segment in segments:
        ind_vars = indvars[indvar_dict[name]]
        name = str(name)
        segment = segment[segment[depvar_name]>0]
        if len(segment[depvar_name]) > max_segment_size:
            segment = segment.ix[np.random.choice(segment.index, max_segment_size,replace=False)]
        depvar = segment[depvar_name].apply(np.log)
        est_data = pd.DataFrame(index=segment.index)
        for varname in indvars:
            est_data[varname] = segment[varname]
        est_data = est_data.fillna(0)
        est_data = sm.add_constant(est_data,prepend=False)
        tmp_outcsv, tmp_outtitle, tmp_coeffname = output_csv%name, output_title%name, coeff_name%name
        print "Estimating hedonic for %s with %d observations" % (name,len(segment.index))

        model = sm.OLS(depvar,est_data)
        results = model.fit()
        print results.summary()
        print ' '
        tmp_outcsv = output_csv%name
        tmp_outtitle = output_title%name
        misc.resultstocsv((results.rsquared,results.rsquared_adj),est_data.columns,
                            zip(results.params,results.bse,results.tvalues),tmp_outcsv,hedonic=1,
                            tblname=output_title)

        dset.store_coeff(tmp_coeffname,results.params.values,results.params.index)