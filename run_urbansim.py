__author__ = 'jmartinez'
from drcog.models import urbansim2

##model configurations

name = 'baseline'
export_buildings_to_urbancanvas = False
base_year = 2011
forecast_year = 2040
fixed_seed = True
random_seed = 1
indicator_configuration = {'years_to_run': [2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025, 2026, 2027, 2028, 2029, 2030, 2031, 2032, 2033, 2034, 2035, 2036, 2037, 2038, 2039, 2040], 'export_indicators': True, 'indicator_output_directory': 'C:\\\\urbansim\\\\data\\\\drcog2\\\\runs'}
core_components_to_run = {'Price':True, 'Developer':True, 'HLCM':True, 'ELCM': True}
household_transition = {'Enabled': True, 'scaling_factor': 1.0, 'control_totals_table': 'annual_household_control_totals'}
household_relocation = {'Enabled': True, 'relocation_rates_table': 'annual_household_relocation_rates', 'scaling_factor': 1.0}
employment_transition = {'Enabled': True, 'scaling_factor': 1.0, 'control_totals_table': 'annual_employment_control_totals'}
elcm_configuration = {'scaling_factor': 1.0, 'building_sqft_per_job_table': 'building_sqft_per_job'}
developer_configuration = {'outside_ugb_allowable_density': 1.0, 'uga_policies': False, 'min_building_sqft': 400, 'enforce_ugb': False, 'min_lot_sqft': 500, 'enforce_environmental_constraints': True, 'zonal_levers': True, 'max_allowable_far_field_name': 'far', 'profit_factor': 1.0, 'enforce_allowable_use_constraints': True, 'land_property_acquisition_cost_factor': 1.0, 'inside_uga_allowable_density': 1.0}
table_swapping = {'swap_dist_rail': False, 'new_dist_rail_file': 'C:\\\\urbansim\\\\data\\\\swap\\\\parcel_dist_rail.csv', 'year': 2020, 'swap_skims': False, 'new_skim_file': 'C:\\\\urbansim\\\\data\\\\swap\\\\travel_data.csv'}
travel_model_configuration1 = {'tm_input_dir': 'C:\\\\urbansim\\\\data\\\\travel_model\\\\2015', 'export_to_tm': False, 'year': 2015}
travel_model_configuration2 = {'tm_input_dir': 'C:\\\\urbansim\\\\data\\\\travel_model\\\\2020', 'export_to_tm': False, 'year': 2020}
travel_model_configuration3 = {'tm_input_dir': 'C:\\\\urbansim\\\\data\\\\travel_model\\\\2025', 'export_to_tm': False, 'year': 2025}
travel_model_configuration4 = {'tm_input_dir': 'C:\\\\urbansim\\\\data\\\\travel_model\\\\2030', 'export_to_tm': False, 'year': 2030}
travel_model_configuration5 = {'tm_input_dir': 'C:\\\\urbansim\\\\data\\\\travel_model\\\\2035', 'export_to_tm': False, 'year': 2035}
travel_model_configuration6 = {'tm_input_dir': 'C:\\\\urbansim\\\\data\\\\travel_model\\\\2040', 'export_to_tm': False, 'year': 2040}

##run model

if __name__ == '__main__':
    import cProfile
    urbansim = urbansim2.Urbansim2()

    fnc = 'urbansim.run(name=name, export_buildings_to_urbancanvas=export_buildings_to_urbancanvas, base_year=base_year,' +\
                 'forecast_year=forecast_year, fixed_seed=fixed_seed, random_seed=random_seed, indicator_configuration=indicator_configuration,' +\
                 'core_components_to_run=core_components_to_run, household_transition=household_transition,' +\
                 'household_relocation=household_relocation,employment_transition=employment_transition,' +\
                 'elcm_configuration=elcm_configuration, developer_configuration=developer_configuration,' +\
                 'table_swapping=table_swapping, travel_model_configuration1=travel_model_configuration1,' +\
                 'travel_model_configuration2=travel_model_configuration2, travel_model_configuration3=travel_model_configuration3,' +\
                 'travel_model_configuration4=travel_model_configuration4, travel_model_configuration5=travel_model_configuration5,' +\
                 'travel_model_configuration6=travel_model_configuration6)'

    cProfile.run(fnc, 'c:/users/jmartinez/documents/projects/urbansim/cprofile/urbansim')
