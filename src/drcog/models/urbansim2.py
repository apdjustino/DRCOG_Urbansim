from opus_core.model import Model
from opus_core.logger import logger
import numpy as np, pandas as pd, os, time
from drcog.models import elcm_simulation, hlcm_simulation_income, regression_model_simulation, dataset, refiner, new_refiner, gwr_hedonic, regression_model_simulation_nr
from drcog.variables import variable_library, indicators, urbancanvas_export
from drcog.travel_model import export_zonal_file
from urbandeveloper import proforma_developer_model
from synthicity.utils import misc

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

        #Keep track of unplaced agents by year
        unplaced_hh = []
        unplaced_emp = []

        #UrbanCanvas scenario id, replaced by db-retrieved value during export step
        urbancanvas_scenario_id = 0

        #####Residential Buildings#####
        new_refiner.add_res_buildings(dset)

        #####Non-Residential Buildings#####
        new_refiner.add_non_res_buildings(dset)


          # Load coefficients for hedonic regression
        segment=[5,9,17,18,22]
        indvars=['stories',  'ln_nres_sqft',  'high_land_area', 'year0', 'year10','year20', 'ln_dist_bus','ln_sqft_zone',
          'ln_sqft_out', 'far', 'ln_stories_zone', 'ln_stories_out', 'ln_emp_10']
        dset_hed_nr=gwr_hedonic.estimate_zone(dset, indvars, segment)

        segment=[2,3,20,24]
        indvars=['stories',  'ln_res_sqft',  'high_land_area', 'year0', 'year10','year20', 'ln_dist_bus','ln_sqft_zone',
          'ln_sqft_out', 'far', 'ln_stories_zone', 'ln_stories_out', 'ln_emp_10']
        dset_hed_r=gwr_hedonic.estimate_zone(dset, indvars, segment)


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

            #################     HLCM SIMULATION
            if core_components_to_run['HLCM']:
                logger.log_status('HLCM simulation.')
                alternatives = dset.buildings[(dset.buildings.residential_units>0)]
                hlcm_simulation_income.simulate(dset, year=sim_year,depvar = 'building_id',alternatives=alternatives,simulation_table = 'households',output_names = ("drcog-coeff-hlcm-%s.csv","DRCOG HOUSEHOLD LOCATION CHOICE MODELS (%s)","hh_location_%s","household_building_ids"),
                                         agents_groupby= ['income_3_tenure',],transition_config = {'Enabled':True,'control_totals_table':'annual_household_control_totals','scaling_factor':1.0},
                                         relocation_config = {'Enabled':True,'relocation_rates_table':'annual_household_relocation_rates','scaling_factor':1.0},)

            ############     DEMAND-SIDE REFINEMENT
            refiner.run(dset, sim_year)

            ############     REPM SIMULATION
            if core_components_to_run['Price']:
                # logger.log_status('REPM simulation.')
                # #Residential
                # regression_model_simulation.simulate(dset, year=sim_year, output_varname='unit_price_residential',simulation_table='buildings', output_names = ["drcog-coeff-reshedonic-%s.csv","DRCOG RESHEDONIC MODEL (%s)","resprice_%s"],
                #                                      agents_groupby = 'building_type_id', segment_ids = [2,3,20,24])
                # #Non-residential
                # regression_model_simulation.simulate(dset, year=sim_year,output_varname='unit_price_non_residential', simulation_table='buildings', output_names = ["drcog-coeff-nrhedonic-%s.csv","DRCOG NRHEDONIC MODEL (%s)","nrprice_%s"],
                #                                      agents_groupby = 'building_type_id', segment_ids = [5,8,11,16,17,18,21,23,9,22])
                logger.log_status('REPM simulation.')
                #Residential
                """"
                regression_model_simulation.simulate(dset, year=sim_year, output_varname='unit_price_res_sqft',simulation_table='buildings', output_names = ["drcog-coeff-reshedonic-%s.csv","DRCOG RESHEDONIC MODEL (%s)","resprice_%s"],
                                                     agents_groupby = 'building_type_id', segment_ids = [2,3,20,24])
                """""

                segment=[2,3,20,24]
                indvars=['stories',  'ln_res_sqft',  'high_land_area', 'year0', 'year10','year20', 'ln_dist_bus','ln_sqft_zone',
          'ln_sqft_out', 'far', 'ln_stories_zone', 'ln_stories_out', 'ln_emp_10']
                regression_model_simulation_nr.simulate(dset,dset_hed_r,"unit_price_res_sqft", indvars,sim_year, segment)
                #Non-residential
                """
                regression_model_simulation.simulate(dset, year=sim_year,output_varname='unit_price_non_residential', simulation_table='buildings', output_names = ["drcog-coeff-nrhedonic-%s.csv","DRCOG NRHEDONIC MODEL (%s)","nrprice_%s"],
                                                     agents_groupby = 'building_type_id', segment_ids = [5,8,11,16,17,18,21,23,9,22])
                print dset.buildings.unit_price_non_residential.mean()
                """

                segment=[5,9,17,18,22]
                indvars=['stories',  'ln_nres_sqft',  'high_land_area', 'year0', 'year10','year20', 'ln_dist_bus','ln_sqft_zone',
          'ln_sqft_out', 'far', 'ln_stories_zone', 'ln_stories_out', 'ln_emp_10']
                regression_model_simulation_nr.simulate(dset,dset_hed_nr,"unit_price_non_residential", indvars,sim_year, segment)

            ############     DEVELOPER SIMULATION
            elasticity_res = 0
            elasticity_non_res = 0

            if core_components_to_run['Developer']:
                logger.log_status('Proforma simulation.')
                buildings, newbuildings = proforma_developer_model.run(dset,hh_zone1,emp_zone1,developer_configuration,sim_year, elasticity_res=elasticity_res, elasticity_non_res=elasticity_non_res)
                #import pdb; pdb.set_trace()
                dset.d['buildings'] = pd.concat([buildings,newbuildings])
                dset.buildings.index.name = 'building_id'


                ############      GENERATE PRICES/RENTS FOR NEW BUILDINGS

            #create variables for hedonic regressions:
            #having a problem passing newbuildings as a parameter, so this dumps to csv to re-load into pandas
            dset.newbuildings = newbuildings

            #newbuildings.to_csv('C:/users/jmartinez/documents/raw_newbuildings.csv')

            new_buildings = variable_library.calculate_variables_hedonic(dset)
            #residential
            regression_model_simulation.simulate_new_building(dset, sim_year, new_buildings, output_varname='unit_price_residential', output_names = ["drcog-coeff-reshedonic-%s.csv","DRCOG RESHEDONIC MODEL (%s)","resprice_%s"],
                                                     agents_groupby = 'building_type_id', segment_ids = [2,3,20,24])

            #non-residential
            regression_model_simulation.simulate_new_building(dset, sim_year, new_buildings, output_varname='unit_price_non_residential', output_names = ["drcog-coeff-reshedonic-%s.csv","DRCOG RESHEDONIC MODEL (%s)","nrprice_%s"],
                                                     agents_groupby = 'building_type_id', segment_ids = [5,8,11,16,17,18,21,23,9,22])

            #new_buildings.to_csv('C:/users/jmartinez/documents/Test Zones/parcel_zones_buildings_joined.csv')
            newbuildings['unit_price_residential']=new_buildings['unit_price_residential'].values
            newbuildings['unit_price_non_residential']= new_buildings['unit_price_non_residential'].values



            dset.d['buildings'] = pd.concat([buildings,newbuildings])

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