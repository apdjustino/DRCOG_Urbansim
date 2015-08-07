from opus_core.model import Model
from opus_core.logger import logger
import numpy as np, pandas as pd, os, time
from drcog.models import elcm_simulation, hlcm_simulation_v2, regression_model_simulation,census_model_simulation, dataset, refiner, new_refiner, new_elcm_model
from drcog.variables import variable_library, indicators , urbancanvas_export, fnc_new_variable_library
from drcog.travel_model import export_zonal_file
from urbandeveloper import proforma_developer_model
from synthicity.utils import misc
import multiprocessing as mp
from functools import partial
import cProfile

class Urbansim2(Model):
    """Runs an UrbanSim2 scenario
    """
    model_name = "UrbanSim2"
    
    def __init__(self,scenario='Base Scenario'):
        self.scenario = scenario

    def run(self, name=None, export_buildings_to_urbancanvas=False, base_year=2010, forecast_year=None, fixed_seed=True, random_seed=1, indicator_configuration=None, core_components_to_run=None, household_transition=None,household_relocation=None,employment_transition=None, elcm_configuration=None, developer_configuration=None, table_swapping=None, travel_model_configuration1=None, travel_model_configuration2=None, travel_model_configuration3=None, travel_model_configuration4=None, travel_model_configuration5=None, travel_model_configuration6=None):
        """Runs an UrbanSim2 scenario 
        """
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

        coeff_store = pd.HDFStore(os.path.join(misc.data_dir(),'coeffs_res.h5'))
        dset.coeffs_res = coeff_store.coeffs_res.copy()
        coeff_store.close()

        #Keep track of unplaced agents by year
        unplaced_hh = []
        unplaced_emp = []
        
        #UrbanCanvas scenario id, replaced by db-retrieved value during export step
        urbancanvas_scenario_id = 0

                #####Residential Buildings#####
        new_refiner.add_res_buildings(dset)

        #####Non-Residential Buildings#####
        new_refiner.add_non_res_buildings(dset)
        
        for sim_year in range(base_year,forecast_year+1):
            print 'Simulating year ' + str(sim_year)
            logger.log_status(sim_year)

            ##Variable Library calculations
            variable_library.calculate_variables(dset)

            # p, b, emp_hh, z = self.gen_variables(dset)
            #
            # hh = emp_hh[1]
            # e = emp_hh[0]
            #
            # p['nonres_far'] = (b.groupby('parcel_id').non_residential_sqft.sum()/p.acres).apply(np.log1p)
            # p['ln_units_per_acre'] = (b.groupby('parcel_id').residential_units.sum()/p.acres).apply(np.log1p)
            #
            # z['residential_sqft_zone'] = b.groupby('zone_id').residential_sqft.sum()
            # z['percent_sf'] = b[b.btype_hlcm==3].groupby('zone_id').residential_units.sum()*100.0/(b.groupby('zone_id').residential_units.sum())
            # z['ln_avg_land_value_per_sqft_zone'] = p.groupby('zone_id').land_value_per_sqft.mean().apply(np.log1p)
            # z['emp_sector1'] = e[e.sector_id_six==1].groupby('zone_id').employees.sum()
            # z['emp_sector2'] = e[e.sector_id_six==2].groupby('zone_id').employees.sum()
            # z['emp_sector3'] = e[e.sector_id_six==3].groupby('zone_id').employees.sum()
            # z['emp_sector4'] = e[e.sector_id_six==4].groupby('zone_id').employees.sum()
            # z['emp_sector5'] = e[e.sector_id_six==5].groupby('zone_id').employees.sum()
            # z['emp_sector6'] = e[e.sector_id_six==6].groupby('zone_id').employees.sum()
            #
            # z['ln_emp_sector1_within_15min'] = dset.compute_range(z.emp_sector1,15.0).apply(np.log1p)
            # z['ln_emp_sector2_within_15min'] = dset.compute_range(z.emp_sector2,15.0).apply(np.log1p)
            # z['ln_emp_sector3_within_10min'] = dset.compute_range(z.emp_sector3,15.0).apply(np.log1p)
            # z['ln_emp_sector3_within_15min'] = dset.compute_range(z.emp_sector3,15.0).apply(np.log1p)
            # z['ln_emp_sector3_within_20min'] = dset.compute_range(z.emp_sector3,20.0).apply(np.log1p)
            # z['ln_emp_sector4_within_15min'] = dset.compute_range(z.emp_sector4,15.0).apply(np.log1p)
            # z['ln_emp_sector5_within_15min'] = dset.compute_range(z.emp_sector5,15.0).apply(np.log1p)
            # z['ln_emp_sector6_within_15min'] = dset.compute_range(z.emp_sector6,15.0).apply(np.log1p)
            #
            #
            #
            # dset.parcels = p
            # dset.establishments = e
            # dset.households = hh
            # dset.zones = z
            #
            #
            # pz = pd.merge(p.reset_index(),z,left_on='zone_id',right_index=True)
            # pz = pz.set_index('parcel_id')
            # #merge buildings with parcels/zones
            # del b['county_id']
            # del b['zone_id']
            # bpz = pd.merge(b,pz,left_on='parcel_id',right_index=True)
            # bpz['residential_units_capacity'] = bpz.parcel_sqft/1500 - bpz.residential_units
            # bpz.loc[bpz.residential_units_capacity < 0, "residential_units_capacity"] = 0  # corrected chained index error
            # dset.d['buildings'] = bpz
            #


            #Record pre-demand model zone-level household/job totals
            hh_zone1 = dset.fetch('households').groupby('zone_id').size()
            emp_zone1 = dset.fetch('establishments').groupby('zone_id').employees.sum()
            
            ############     ELCM SIMULATION
            if core_components_to_run['ELCM']:
                logger.log_status('ELCM simulation.')
                alternatives = dset.buildings[(dset.buildings.non_residential_sqft>0)]
                new_elcm_model.simulate(dset, year=sim_year,depvar = 'building_id',alternatives=alternatives,simulation_table = 'establishments',output_names = ("drcog-coeff-elcm-%s.csv","DRCOG EMPLOYMENT LOCATION CHOICE MODELS (%s)","emp_location_%s","establishment_building_ids"),
                                         agents_groupby= ['sector_id_retail_agg',],transition_config = {'Enabled':True,'control_totals_table':'annual_employment_control_totals','scaling_factor':1.0})

            #################     HLCM SIMULATION
            if core_components_to_run['HLCM']:
                logger.log_status('HLCM simulation.')
                alternatives = dset.buildings[(dset.buildings.residential_units>0)]
                hlcm_simulation_v2.simulate(dset, year=sim_year,depvar = 'building_id',alternatives=alternatives,simulation_table = 'households',output_names = ("drcog-coeff-hlcm-%s.csv","DRCOG HOUSEHOLD LOCATION CHOICE MODELS (%s)","hh_location_%s","household_building_ids"),
                                         agents_groupby= ['income_3_tenure',],transition_config = {'Enabled':True,'control_totals_table':'annual_household_control_totals','scaling_factor':1.0},
                                         relocation_config = {'Enabled':True,'relocation_rates_table':'annual_household_relocation_rates','scaling_factor':1.0},)
                                         
            ############     DEMAND-SIDE REFINEMENT
            refiner.run(dset, sim_year)
            # refiner_fnc = "refiner.run(dset, sim_year)"
            #cProfile.runctx(refiner_fnc, locals={'dset':dset, 'sim_year':sim_year}, globals={'refiner': refiner}, filename='c:/users/jmartinez/documents/refiner_time')

            ############     REPM SIMULATION
            if core_components_to_run['Price']:
                logger.log_status('REPM simulation.')
                #Residential
                census_model_simulation.simulate_residential(dset, 'unit_price_res_sqft', 'school_district_id', 10, sim_year)

                #Non-residential                                    
                regression_model_simulation.simulate(dset, year=sim_year,output_varname='unit_price_non_residential', simulation_table='buildings', output_names = ["drcog-coeff-nrhedonic-%s.csv","DRCOG NRHEDONIC MODEL (%s)","nrprice_%s"],
                                                     agents_groupby = 'building_type_id', segment_ids = [5,8,11,16,17,18,21,23,9,22])
            
            ############     DEVELOPER SIMULATION
            if core_components_to_run['Developer']:
                logger.log_status('Proforma simulation.')
                buildings, newbuildings = proforma_developer_model.run(dset,hh_zone1,emp_zone1,developer_configuration,sim_year)
                #import pdb; pdb.set_trace()
                dset.d['buildings'] = pd.concat([buildings,newbuildings])
                dset.buildings.index.name = 'building_id'
            
            ############   INDICATORS
            if indicator_configuration['export_indicators']:
                unplaced_hh.append((dset.households.building_id==-1).sum())
                unplaced_emp.append(dset.establishments[dset.establishments.building_id==-1].employees.sum())
                if sim_year in indicator_configuration['years_to_run']:
                    logger.log_status('Exporting indicators')
                    indicators.run(dset, indicator_configuration['indicator_output_directory'], sim_year)
                    logger.log_status('unplaced hh')
                    logger.log_status(unplaced_hh)
                    logger.log_status('unplaced emp')
                    logger.log_status(unplaced_emp)
                    
            ############     TRAVEL MODEL
            export_zonal_file.export_zonal_file_to_tm(dset,sim_year,logger,tm_config=[travel_model_configuration1,travel_model_configuration2,travel_model_configuration3,travel_model_configuration4,travel_model_configuration5,travel_model_configuration6])
                    
            ############     SWAPPER
            if sim_year == table_swapping['year']:
                if table_swapping['swap_skims']:
                    logger.log_status('Swapping skims')
                    td2 = pd.read_csv(table_swapping['new_skim_file'], index_col=['from_zone_id','to_zone_id'])
                    dset.d['travel_data'] = td2
                if table_swapping['swap_dist_rail']:
                    logger.log_status('Swapping parcel distance to rail')
                    p2 = pd.read_csv(table_swapping['new_dist_rail_file'], index_col=['parcel_id'])
                    dset.d['parcels']['dist_rail'] = p2.dist_rail
            
            ############      URBANCANVAS
            if export_buildings_to_urbancanvas:
                logger.log_status('Exporting %s buildings to Urbancanvas database for project %s and year %s.' % (newbuildings.index.size,urbancanvas_scenario_id,sim_year))
                urbancanvas_scenario_id = urbancanvas_export.export_to_urbancanvas(newbuildings, sim_year, urbancanvas_scenario_id)
                
        elapsed = time.time() - seconds_start
        print "TOTAL elapsed time: " + str(elapsed) + " seconds."

    def gen_variables(self, dset):
        p, b, hh_est, hh, e, z, travel_data, bsqft_job = fnc_new_variable_library.get_tables(dset)

        out_q1 = mp.Queue()
        out_q2 = mp.Queue()
        out_q3 = mp.Queue()
        out_q4 = mp.Queue()

        map_parcel = partial(fnc_new_variable_library.process_parcels, p)
        map_building = partial(fnc_new_variable_library.process_buildings, p, b, bsqft_job, e)
        map_hh_e = partial(fnc_new_variable_library.process_hh_estabs, hh_est, hh, e, b)
        map_zones = partial(fnc_new_variable_library.process_zones, z, hh ,e, b, p, travel_data)

        p1 = mp.Process(target=map_parcel, args=(out_q1,))
        p2 = mp.Process(target=map_building, args=(out_q2,))
        p3 = mp.Process(target=map_hh_e, args=(out_q3,))
        p4 = mp.Process(target=map_zones, args=(out_q4,))

        #jobs.append(p1)
        p1.start()
        #jobs.append(p2)
        p2.start()
        #jobs.append(p3)
        p3.start()
        #jobs.append(p4)
        p4.start()

        #p1.join()
        #p2.join()
        #p3.join()
        #p4.join()

        return out_q1.get(), out_q2.get(), out_q3.get(), out_q4.get()


