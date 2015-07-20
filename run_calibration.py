__author__ = 'jmartinez'
from drcog.models import calibration

##model configurations

name = 'baseline'
export_buildings_to_urbancanvas = False
base_year = 2011
forecast_year = 2015
fixed_seed = True
random_seed = 1
core_components_to_run = {'Price':True, 'Developer':True, 'HLCM':True, 'ELCM': True}
household_transition = {'Enabled': True, 'scaling_factor': 1.0, 'control_totals_table': 'annual_household_control_totals'}
household_relocation = {'Enabled': True, 'relocation_rates_table': 'annual_household_relocation_rates', 'scaling_factor': 1.0}
employment_transition = {'Enabled': True, 'scaling_factor': 1.0, 'control_totals_table': 'annual_employment_control_totals'}
elcm_configuration = {'scaling_factor': 1.0, 'building_sqft_per_job_table': 'building_sqft_per_job'}
developer_configuration = {'outside_ugb_allowable_density': 1.0, 'uga_policies': False, 'min_building_sqft': 400, 'enforce_ugb': False, 'min_lot_sqft': 500, 'enforce_environmental_constraints': True, 'zonal_levers': True, 'max_allowable_far_field_name': 'far', 'profit_factor': 1.0, 'enforce_allowable_use_constraints': True, 'land_property_acquisition_cost_factor': 1.0, 'inside_uga_allowable_density': 1.0}
calibration_configuration = {'coefficient_step_size': 0.05, 'match_target_within': 0.02, 'iterations': 50}
emp_targets = {'emp_8014_target': 0.0473, 'emp_8019_target': 0.0001, 'emp_8039_target': 0.0139, 'emp_8013_target': 0.0737, 'emp_8035_target': 0.1094, 'emp_8123_target': 0.0197, 'emp_8031_target': 0.2435, 'emp_8047_target': 0.0005, 'emp_8001_target': 0.1511, 'emp_8005_target': 0.2232, 'emp_8059_target': 0.1178}
hh_targets = {'hh_8001_target': 0.198, 'hh_8059_target': 0.099, 'hh_8035_target': 0.142, 'hh_8031_target': 0.165, 'hh_8039_target': 0.014, 'hh_8047_target': 0.002, 'hh_8013_target': 0.105, 'hh_8014_target': 0.032, 'hh_8005_target': 0.205, 'hh_8019_target': 0.002, 'hh_8123_target': 0.037}
ru_targets = {'ru_8031_target': 0.165, 'ru_8014_target': 0.032, 'ru_8039_target': 0.014, 'ru_8035_target': 0.142, 'ru_8059_target': 0.099, 'ru_8001_target': 0.198, 'ru_8013_target': 0.105, 'ru_8047_target': 0.002, 'ru_8005_target': 0.205, 'ru_8123_target': 0.037, 'ru_8019_target': 0.002}
nrsqft_targets = {'nr_8001_target': 0.1511, 'nr_8035_target': 0.1094, 'nr_8013_target': 0.0737, 'nr_8014_target': 0.0473, 'nr_8123_target': 0.0197, 'nr_8039_target': 0.0139, 'nr_8019_target': 0.0001, 'nr_8031_target': 0.2435, 'nr_8059_target': 0.1178, 'nr_8047_target': 0.0005, 'nr_8005_target': 0.2232}

if __name__ == '__main__':
    import cProfile
    calib = calibration.Urbansim2()

    fnc = 'calib.run(name=name, export_buildings_to_urbancanvas=export_buildings_to_urbancanvas, base_year=base_year,' +\
                 'forecast_year=forecast_year, fixed_seed=fixed_seed, random_seed=random_seed,' +\
                 'core_components_to_run=core_components_to_run, household_transition=household_transition,' +\
                 'household_relocation=household_relocation,employment_transition=employment_transition,' +\
                 'elcm_configuration=elcm_configuration, developer_configuration=developer_configuration,' +\
                 'calibration_configuration=calibration_configuration, hh_targets=hh_targets, emp_targets=emp_targets, ru_targets=ru_targets, nrsqft_targets=nrsqft_targets)'

    cProfile.run(fnc, 'c:/users/jmartinez/documents/projects/urbansim/cprofile/calibration')