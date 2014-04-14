import statsmodels.api as sm
import pandas as pd, numpy as np
from synthicity.utils import misc

def simulate(dset,year,output_varname = 'price',simulation_table = 'buildings',output_names=None,agents_groupby='building_type_id', segment_ids=[2,3,20,24]):

    output_csv, output_title, coeff_name = output_names

    choosers = dset.fetch(simulation_table)

    choosers = choosers[np.in1d(choosers[agents_groupby],segment_ids)]

    segments = choosers.groupby(agents_groupby)

    simrents = []

    for name, segment in segments:
        tmp_outcsv, tmp_outtitle, tmp_coeffname = output_csv%name, output_title%name, coeff_name%name
        ind_vars = dset.coeffs[(tmp_coeffname, 'fnames')][np.invert(dset.coeffs[(tmp_coeffname, 'fnames')].isnull().values)].values.tolist()
        est_data = pd.DataFrame(index=segment.index)
        for varname in ind_vars:
            if varname != 'const':
                est_data[varname] = segment[varname]
        est_data = est_data.fillna(0)
        est_data = sm.add_constant(est_data,prepend=False)
        print "Generating rents on %d buildings" % (est_data.shape[0])
        vec = dset.load_coeff(tmp_coeffname)
        vec = np.reshape(vec,(vec.size,1))
        rents = est_data.dot(vec).astype('f4')
        rents = rents.apply(np.exp)
        simrents.append(rents[rents.columns[0]])
    simrents = pd.concat(simrents)
    dset.buildings[output_varname] = simrents.reindex(dset.buildings.index)
    dset.store_attr(output_varname,year,simrents)