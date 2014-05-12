from opus_core.model import Model
from opus_core.logger import logger
import numpy as np, pandas as pd, os, time
from drcog.models import elcm_simulation, hlcm_simulation, regression_model_simulation, dataset
from drcog.variables import variable_library, indicators
from drcog.travel_model import export_zonal_file
from urbandeveloper import proforma_developer_model
from synthicity.utils import misc

class Urbansim2(Model):
    """Runs an UrbanSim2 scenario
    """
    model_name = "UrbanSim2"
    
    def __init__(self,scenario='Base Scenario'):
        self.scenario = scenario

    def run(self, name=None, export_buildings_to_urbancanvas=False, base_year=2010, forecast_year=None, fixed_seed=True, random_seed=1, export_indicators=True, indicator_output_directory='C:/opus/data/drcog2/runs', core_components_to_run=None, household_transition=None,household_relocation=None,employment_transition=None, elcm_configuration=None, developer_configuration=None, calibration_configuration=None, hh_targets=None, ru_targets=None,  emp_targets=None, nrsqft_targets=None):
        """Runs an UrbanSim2 scenario 
        """
        
        ##Calibration targets
        #resunit_targets = np.array([.198,.205,.105,.032,.002,.165,.142,.014,.002,.099,.037])
        #hh_targets = np.array([.198,.205,.105,.032,.002,.165,.142,.014,.002,.099,.037])
        #emp_targets = np.array([0.1511,0.2232,0.0737,0.0473,0.0001,0.2435,0.1094,0.0139,0.0005,0.1178,0.0197])
        #nonres_targets = np.array([0.1511,0.2232,0.0737,0.0473,0.0001,0.2435,0.1094,0.0139,0.0005,0.1178,0.0197])
        hh_targets = np.array([hh_targets['hh_8001_target'],hh_targets['hh_8005_target'],hh_targets['hh_8013_target'],hh_targets['hh_8014_target'],hh_targets['hh_8019_target'],hh_targets['hh_8031_target'],hh_targets['hh_8035_target'],hh_targets['hh_8039_target'],hh_targets['hh_8047_target'],hh_targets['hh_8059_target'],hh_targets['hh_8123_target']])
        resunit_targets = np.array([ru_targets['ru_8001_target'],ru_targets['ru_8005_target'],ru_targets['ru_8013_target'],ru_targets['ru_8014_target'],ru_targets['ru_8019_target'],ru_targets['ru_8031_target'],ru_targets['ru_8035_target'],ru_targets['ru_8039_target'],ru_targets['ru_8047_target'],ru_targets['ru_8059_target'],ru_targets['ru_8123_target']])
        emp_targets = np.array([emp_targets['emp_8001_target'],emp_targets['emp_8005_target'],emp_targets['emp_8013_target'],emp_targets['emp_8014_target'],emp_targets['emp_8019_target'],emp_targets['emp_8031_target'],emp_targets['emp_8035_target'],emp_targets['emp_8039_target'],emp_targets['emp_8047_target'],emp_targets['emp_8059_target'],emp_targets['emp_8123_target']])
        nonres_targets = np.array([nrsqft_targets['nr_8001_target'],nrsqft_targets['nr_8005_target'],nrsqft_targets['nr_8013_target'],nrsqft_targets['nr_8014_target'],nrsqft_targets['nr_8019_target'],nrsqft_targets['nr_8031_target'],nrsqft_targets['nr_8035_target'],nrsqft_targets['nr_8039_target'],nrsqft_targets['nr_8047_target'],nrsqft_targets['nr_8059_target'],nrsqft_targets['nr_8123_target']])
        county_id = np.array([8001,8005,8013,8014,8019,8031,8035,8039,8047,8059,8123])
        targets = pd.DataFrame({'county_id':county_id,'resunit_target':resunit_targets,'hh_target':hh_targets,'emp_target':emp_targets,'nonres_target':nonres_targets})
        delta = calibration_configuration['coefficient_step_size']
        margin = calibration_configuration['match_target_within']
        iterations = calibration_configuration['iterations']
        
        for it in range(iterations):
            logger.log_status('Calibration iteration: ' + str(it))
            
            
            logger.log_status('Starting UrbanSim2 run.')
            dset = dataset.DRCOGDataset(os.path.join(misc.data_dir(),'drcog.h5'))
            seconds_start = time.time()
            if fixed_seed:
                logger.log_status('Running with fixed random seed.')
                np.random.seed(random_seed)
                
            #Load estimated coefficients
            coeff_store = pd.HDFStore(os.path.join(misc.data_dir(),'coeffs.h5'))
            dset.coeffs = coeff_store.coeffs.copy()
            coeff_store.close()
            
            #Keep track of unplaced agents by year
            unplaced_hh = []
            unplaced_emp = []
            
            for sim_year in range(base_year,forecast_year+1):
                print 'Simulating year ' + str(sim_year)
                logger.log_status(sim_year)

                ##Variable Library calculations
                variable_library.calculate_variables(dset)
                
                #Record pre-demand model zone-level household/job totals
                hh_zone1 = dset.fetch('households').groupby('zone_id').size()
                emp_zone1 = dset.fetch('establishments').groupby('zone_id').employees.sum()
                        
                ############     ELCM SIMULATION
                if core_components_to_run['ELCM']:
                    logger.log_status('ELCM simulation.')
                    alternatives = dset.buildings[(dset.buildings.non_residential_sqft>0)]
                    elcm_simulation.simulate(dset, year=sim_year,depvar = 'building_id',alternatives=alternatives,simulation_table = 'establishments',output_names = ("drcog-coeff-elcm-%s.csv","DRCOG EMPLOYMENT LOCATION CHOICE MODELS (%s)","emp_location_%s","establishment_building_ids"),
                                             agents_groupby= ['sector_id_retail_agg',],transition_config = {'Enabled':True,'control_totals_table':'annual_employment_control_totals','scaling_factor':1.0})
                        
                #################     HLCM simulation
                if core_components_to_run['HLCM']:
                    logger.log_status('HLCM simulation.')
                    alternatives = dset.buildings[(dset.buildings.residential_units>0)]
                    hlcm_simulation.simulate(dset, year=sim_year,depvar = 'building_id',alternatives=alternatives,simulation_table = 'households',output_names = ("drcog-coeff-hlcm-%s.csv","DRCOG HOUSEHOLD LOCATION CHOICE MODELS (%s)","hh_location_%s","household_building_ids"),
                                             agents_groupby= ['income_3_tenure',],transition_config = {'Enabled':True,'control_totals_table':'annual_household_control_totals','scaling_factor':1.0},
                                             relocation_config = {'Enabled':True,'relocation_rates_table':'annual_household_relocation_rates','scaling_factor':1.0},)

                ############     REPM SIMULATION
                if core_components_to_run['Price']:
                    logger.log_status('REPM simulation.')
                    #Residential
                    regression_model_simulation.simulate(dset, year=sim_year, output_varname='unit_price_residential',simulation_table='buildings', output_names = ["drcog-coeff-reshedonic-%s.csv","DRCOG RESHEDONIC MODEL (%s)","resprice_%s"],
                                                         agents_groupby = 'building_type_id', segment_ids = [2,3,20,24])
                    #Non-residential                                    
                    regression_model_simulation.simulate(dset, year=sim_year,output_varname='unit_price_non_residential', simulation_table='buildings', output_names = ["drcog-coeff-nrhedonic-%s.csv","DRCOG NRHEDONIC MODEL (%s)","nrprice_%s"],
                                                         agents_groupby = 'building_type_id', segment_ids = [5,8,11,16,17,18,21,23,9,22])
                    
                ############     DEVELOPER SIMULATION
                if core_components_to_run['Developer']:
                    logger.log_status('Proforma simulation.')
                    buildings, newbuildings = proforma_developer_model.run(dset,hh_zone1,emp_zone1,developer_configuration,sim_year)
                    dset.d['buildings'] = pd.concat([buildings,newbuildings])

                ###########   Indicators
                # if export_indicators:
                    # unplaced_hh.append((dset.households.building_id==-1).sum())
                    # unplaced_emp.append(dset.establishments[dset.establishments.building_id==-1].employees.sum())
                    # if sim_year == forecast_year:
                        # logger.log_status('Exporting indicators')
                        # indicators.run(dset, indicator_output_directory, forecast_year)
                        
                ###########     TRAVEL MODEL
                    # if travel_model_configuration['export_to_tm']:
                        # if sim_year in travel_model_configuration['years_to_run']:
                            # logger.log_status('Exporting to TM')
                            # export_zonal_file.export_zonal_file_to_tm(dset,sim_year,tm_input_dir=travel_model_configuration['tm_input_dir'])
                    
            elapsed = time.time() - seconds_start
            print "TOTAL elapsed time: " + str(elapsed) + " seconds."
            
            
            ###########   Calibration
            logger.log_status('Calibration coefficient updating')
            import math
            hh_submodels = []
            for col in dset.coeffs.columns:
                if col[0].startswith('hh_') and col[1]=='fnames':
                    hh_submodels.append(col[0])
            emp_submodels = []
            for col in dset.coeffs.columns:
                if col[0].startswith('emp_') and col[1]=='fnames':
                    emp_submodels.append(col[0])

            #Record base values for temporal comparison
            hh = dset.store.households
            e = dset.store.establishments
            b = dset.store.buildings
            p = dset.store.parcels.set_index('parcel_id')
            b['county_id'] = p.county_id[b.parcel_id].values
            hh['county_id'] = b.county_id[hh.building_id].values
            e['county_id'] = b.county_id[e.building_id].values
            base_hh_county = hh.groupby('county_id').size()
            base_emp_county = e.groupby('county_id').employees.sum()
            base_ru_county = b.groupby('county_id').residential_units.sum()
            base_nr_county = b.groupby('county_id').non_residential_sqft.sum()
            
            #Calibration indicators
            b = dset.fetch('buildings')
            e = dset.fetch('establishments')
            hh = dset.fetch('households')
            p = dset.fetch('parcels')
            b['county_id'] = p.county_id[b.parcel_id].values
            hh['county_id'] = b.county_id[hh.building_id].values
            e['county_id'] = b.county_id[e.building_id].values
            sim_hh_county = hh.groupby('county_id').size()
            sim_emp_county = e.groupby('county_id').employees.sum()
            sim_ru_county = b.groupby('county_id').residential_units.sum()
            sim_nr_county = b.groupby('county_id').non_residential_sqft.sum()
            hh_diff_county = sim_hh_county - base_hh_county
            emp_diff_county = sim_emp_county - base_emp_county
            ru_diff_county = sim_ru_county - base_ru_county
            nr_diff_county = sim_nr_county - base_nr_county

            prop_growth_emp = emp_diff_county*1.0/emp_diff_county.sum()
            prop_growth_hh = hh_diff_county*1.0/hh_diff_county.sum()
            prop_growth_ru = ru_diff_county*1.0/ru_diff_county.sum()
            prop_growth_nr = nr_diff_county*1.0/nr_diff_county.sum()
            
            county_args = pd.read_csv(os.path.join(misc.data_dir(),'county_calib.csv')).set_index('county_id')
            
            i = 0;j = 0;k = 0;m = 0
            for x in targets.county_id.values:
                cid = int(x)
                print cid
                prop_ru = prop_growth_ru[cid]
                prop_hh = prop_growth_hh[cid]
                prop_emp = prop_growth_emp[cid]
                prop_nonres = prop_growth_nr[cid]
                print 'ru prop is ' + str(prop_ru)
                print 'nsqft prop is ' + str(prop_nonres)
                print 'hh prop is ' + str(prop_hh)
                print 'emp prop is ' + str(prop_emp)
                logger.log_status('ru prop is ' + str(prop_ru))
                logger.log_status('nsqft prop is ' + str(prop_nonres))
                logger.log_status('hh prop is ' + str(prop_hh))
                logger.log_status('emp prop is ' + str(prop_emp))
                target_ru = targets.resunit_target[targets.county_id==cid].values[0]
                target_hh = targets.hh_target[targets.county_id==cid].values[0]
                target_emp = targets.emp_target[targets.county_id==cid].values[0]
                target_nonres = targets.nonres_target[targets.county_id==cid].values[0]
                print 'ru target is ' + str(target_ru)
                print 'nsqft target is ' + str(target_nonres)
                print 'hh target is ' + str(target_hh)
                print 'emp target is ' + str(target_emp)
                logger.log_status('ru target is ' + str(target_ru))
                logger.log_status('nsqft target is ' + str(target_nonres))
                logger.log_status('hh target is ' + str(target_hh))
                logger.log_status('emp target is ' + str(target_emp))
                
                varname = 'county%s' % (cid)
                print varname
                if (prop_ru > (target_ru - margin)) and (prop_ru < (target_ru + margin)):
                    print 'NO ru action.'
                    logger.log_status('NO ru action.')
                    i = i + 1
                elif math.isnan(prop_ru) or (prop_ru < target_ru):
                    county_args.chh_demand_factor[cid] = county_args.chh_demand_factor[cid] + .1
                    county_args.cres_price_factor[cid] = county_args.cres_price_factor[cid] + .1
                    print 'ru action is PLUS'
                    logger.log_status('ru action is PLUS')
                elif prop_ru > target_ru:
                    county_args.chh_demand_factor[cid] = county_args.chh_demand_factor[cid] - .1
                    county_args.cres_price_factor[cid] = county_args.cres_price_factor[cid] - .1
                    print 'ru action is MINUS'
                    logger.log_status('ru action is MINUS')
                    
                if (prop_hh > (target_hh - margin)) and (prop_hh < (target_hh + margin)):
                    print 'NO hh action.'
                    logger.log_status('NO hh action.')
                    j = j + 1
                elif math.isnan(prop_hh) or (prop_hh < target_hh):
                    for submodel in hh_submodels:
                        dset.coeffs[(submodel, 'coeffs')][dset.coeffs[(submodel,'fnames')]==varname] = dset.coeffs[(submodel, 'coeffs')][dset.coeffs[(submodel,'fnames')]==varname] + delta
                    print 'hh action is PLUS'
                    logger.log_status('hh action is PLUS')
                elif prop_hh > target_hh:
                    for submodel in hh_submodels:
                        dset.coeffs[(submodel, 'coeffs')][dset.coeffs[(submodel,'fnames')]==varname] = dset.coeffs[(submodel, 'coeffs')][dset.coeffs[(submodel,'fnames')]==varname] - delta
                    print 'hh action is MINUS'
                    logger.log_status('hh action is MINUS')
                    
                if (prop_emp > (target_emp - margin)) and (prop_emp < (target_emp + margin)):
                    print 'NO emp action.'
                    logger.log_status('NO emp action.')
                    k = k + 1
                elif math.isnan(prop_emp) or (prop_emp < target_emp):
                    for submodel in emp_submodels:
                        dset.coeffs[(submodel, 'coeffs')][dset.coeffs[(submodel,'fnames')]==varname] = dset.coeffs[(submodel, 'coeffs')][dset.coeffs[(submodel,'fnames')]==varname] + delta
                    print 'emp action is PLUS'
                    logger.log_status('emp action is PLUS')
                elif prop_emp > target_emp:
                    for submodel in emp_submodels:
                        dset.coeffs[(submodel, 'coeffs')][dset.coeffs[(submodel,'fnames')]==varname] = dset.coeffs[(submodel, 'coeffs')][dset.coeffs[(submodel,'fnames')]==varname] - delta
                    print 'emp action is MINUS'
                    logger.log_status('emp action is MINUS')
                    
                if (prop_nonres > (target_nonres - margin)) and (prop_nonres < (target_nonres + margin)):
                    print 'NO nonres action.'
                    logger.log_status('NO nonres action.')
                    m = m + 1
                elif math.isnan(prop_nonres) or (prop_nonres < target_nonres):
                    county_args.cemp_demand_factor[cid] = county_args.cemp_demand_factor[cid] + .1
                    county_args.cnonres_price_factor[cid] = county_args.cnonres_price_factor[cid] + .1
                    print 'nonres action is PLUS'
                    logger.log_status('nonres action is PLUS')
                elif prop_nonres > target_nonres:
                    county_args.cemp_demand_factor[cid] = county_args.cemp_demand_factor[cid] - .1
                    county_args.cnonres_price_factor[cid] = county_args.cnonres_price_factor[cid] - .1
                    print 'nonres action is MINUS'
                    logger.log_status('nonres action is MINUS')
                    
            print i,j,k,m
            logger.log_status('Number of hh county targets met: %s' % j)
            logger.log_status('Number of emp county targets met: %s' % k)
            logger.log_status('Number of ru county targets met: %s' % i)
            logger.log_status('Number of nr county targets met: %s' % m)
            ###Save calibrated coefficients at the end of each iteration
            coeff_store_path = os.path.join(misc.data_dir(),'coeffs.h5')
            coeff_store = pd.HDFStore(coeff_store_path)
            coeff_store['coeffs'] = dset.coeffs
            coeff_store.close()
            county_args.to_csv(os.path.join(misc.data_dir(),'county_calib.csv'))