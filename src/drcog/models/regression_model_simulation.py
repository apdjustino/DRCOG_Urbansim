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
        name = int(name) #convert name into interger
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
        try:
            rents = est_data.dot(vec).astype('f4')
        except:
            est_data['const'] = 1
            rents = pd.DataFrame({'rents':np.transpose(np.dot(est_data,vec).astype('f4'))[0]},index=est_data.index)
        rents = rents.apply(np.exp)
        simrents.append(rents[rents.columns[0]])
    simrents = pd.concat(simrents)
    dset.buildings[output_varname] = simrents.reindex(dset.buildings.index)
    dset.store_attr(output_varname,year,simrents)

    #write prices to db
    #table = dset.buildings
    #table_name = simulation_table
    #table_columns = table.columns
    #table['price']=simrents.reindex(dset.buildings.index)
    #table = table.reset_index()
    #table = table[['building_id', 'parcel_id', 'price']]
    #table['_year_added_'] = year
    #writer = db_writer.db_writer()
    #engine = create_engine('postgresql://model_team:m0d3lte@m@lumsrv:5432/urbansim', echo=False)
    #sql_table_name = simulation_table + "_test"
    #writer.write_to_db(engine, table, sql_table_name, chunk_size=100000, if_exists='append')
    #print('Writing to CSV')
    #table.to_csv('C:/urbansim/data/drcog2/raw_data/' + sql_table_name + '.csv', header = true)
    #with open('C:/urbansim/data/drcog2/raw_data/' + table_name + '_' + output_varname + '_' + year.__str__() + '.csv', 'a') as file_to_insert:
    #   table.to_csv(file_to_insert, header = True)
    #    file_to_insert.close()

def simulate_new_building(dset,year,simulation_table, output_varname = 'price',output_names=None,agents_groupby='building_type_id', segment_ids=[2,3,20,24]):

    output_csv, output_title, coeff_name = output_names

    choosers = simulation_table

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
        try:
            rents = est_data.dot(vec).astype('f4')
        except:
            est_data['const'] = 1
            rents = pd.DataFrame({'rents':np.transpose(np.dot(est_data,vec).astype('f4'))[0]},index=est_data.index)
        rents = rents.apply(np.exp)
        simrents.append(rents[rents.columns[0]])
    simrents = pd.concat(simrents)
    #dset.buildings[output_varname] = simrents.reindex(dset.buildings.index)
    #dset.store_attr(output_varname,year,simrents)

    simulation_table[output_varname] = simrents.reindex(simulation_table.index)

    #write prices to db
    #table = dset.buildings
    #table_name = simulation_table
    #table_columns = table.columns
    #table['price']=simrents.reindex(dset.buildings.index)
    #table = table.reset_index()
    #table = table[['building_id', 'parcel_id', 'price']]
    #table['_year_added_'] = year
    #writer = db_writer.db_writer()
    #engine = create_engine('postgresql://model_team:m0d3lte@m@lumsrv:5432/urbansim', echo=False)
    #sql_table_name = simulation_table + "_test"
    #writer.write_to_db(engine, table, sql_table_name, chunk_size=100000, if_exists='append')
    #print('Writing to CSV')
    #table.to_csv('C:/urbansim/data/drcog2/raw_data/' + sql_table_name + '.csv', header = true)
    #with open('C:/urbansim/data/drcog2/raw_data/' + table_name + '_' + output_varname + '_' + year.__str__() + '.csv', 'a') as file_to_insert:
    #   table.to_csv(file_to_insert, header = True)
    #    file_to_insert.close()
