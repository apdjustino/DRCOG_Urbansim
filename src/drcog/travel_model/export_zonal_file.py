import numpy as np, pandas as pd
import pandas.io.sql as sql
import psycopg2
import cStringIO

def export_zonal_file_to_tm(dset,sim_year,logger,tm_config=None):

    years_to_run = []
    tm_launch = []
    export_to_dir = []
    for config in tm_config:
        years_to_run.append(config['year'])
        tm_launch.append(config['export_to_tm'])
        export_to_dir.append(config['tm_input_dir'])
    tm_run_config = pd.DataFrame({'year':years_to_run,'launch_travel_model':tm_launch,'export_to_dir':export_to_dir})

    if (sim_year in tm_run_config.year.values) and (tm_run_config.launch_travel_model[tm_run_config.year==sim_year].values[0] == True):
        logger.log_status('Exporting to TM')
        tm_input_dir = tm_run_config.export_to_dir[tm_run_config.year==sim_year].values[0]

        buildings = dset.fetch('buildings')[['building_type_id','improvement_value','land_area','non_residential_sqft','parcel_id','residential_units','sqft_per_unit','stories','tax_exempt','year_built','bldg_sq_ft','unit_price_non_residential','unit_price_residential','building_sqft_per_job','non_residential_units','base_year_jobs','all_units']]
        establishments = dset.fetch('establishments')
        del establishments['zone_id']
        del establishments['county_id']
        households = dset.fetch('households')
        del households['zone_id']
        del households['county_id']
        parcels = dset.fetch('parcels')
        parcels_urbancen = dset.store.parcels_urbancen.set_index('parcel_id')
        parcels['urbancenter_id'] = parcels_urbancen.urban_cen
        zones = dset.fetch('zones')
        pz = pd.merge(parcels.reset_index(),zones,left_on='zone_id',right_index=True,how='left')
        pz = pz.set_index('parcel_id')
        bpz = pd.merge(buildings,pz,left_on='parcel_id',right_index=True)
        
        ##Merge buildings and parcels
        buildings = pd.merge(buildings,parcels,left_on='parcel_id',right_index=True)

        ##Merge households with bulidings/parcels
        households = pd.merge(households,buildings,left_on='building_id',right_index=True)

        ##Merge establishments with bulidings/parcels
        establishments = pd.merge(establishments,buildings,left_on='building_id',right_index=True)
        
        ##Calculate the income breakpoints
        low_income_breakpoint = households.income.quantile(.11)
        high_income_breakpoint = households.income.quantile(.75)

        ##Tag households according to income bucket
        households['low_inc_hh'] = households.income<low_income_breakpoint
        households['med_inc_hh'] = (households.income>=low_income_breakpoint)*(households.income<high_income_breakpoint)
        households['high_inc_hh'] = households.income>=high_income_breakpoint
        
        ##Zonal household variables
        hh_pop = households.groupby('zone_id').persons.sum()
        avg_hh_size = households.groupby('zone_id').persons.mean()
        tot_hh = households.groupby('zone_id').size()
        low_inc_hh = households[households.low_inc_hh==True].groupby('zone_id').size()
        med_inc_hh = households[households.med_inc_hh==True].groupby('zone_id').size()
        high_inc_hh = households[households.high_inc_hh==True].groupby('zone_id').size()
        
        ##Zonal establishment variables
        total_employment = establishments.groupby('zone_id').employees.sum()
        prod_dist_sectors = [11,21,22,23,31,32,33,42,48,49]
        prod_sectors = [11,21,22,23,31,32,33,42]
        retail_sectors = [44,45,7211,7212,7213,7223]
        retail2_sectors = [44,45,7211,7212,7213,7223,7221,7222,7224]
        restaurant_sectors = [7221,7222,7224]
        service_sectors = [51,52,53,54,55,56,62,81,92]
        service2_sectors = [48,49,51,52,53,54,55,56,62,81,92]
        prod_dist_emp = establishments[np.in1d(establishments.sector_id,prod_dist_sectors)].groupby('zone_id').employees.sum()
        retail_emp = establishments[np.in1d(establishments.sector_id,retail_sectors)].groupby('zone_id').employees.sum()
        service_emp = establishments[np.in1d(establishments.sector_id,service_sectors)].groupby('zone_id').employees.sum()

        education_emp = establishments[establishments.sector_id==61].groupby('zone_id').employees.sum()
        entertainment_emp = establishments[establishments.sector_id==71].groupby('zone_id').employees.sum()
        production_emp = establishments[np.in1d(establishments.sector_id,prod_sectors)].groupby('zone_id').employees.sum()
        restaurant_emp = establishments[np.in1d(establishments.sector_id,restaurant_sectors)].groupby('zone_id').employees.sum()
        retail2_emp = establishments[np.in1d(establishments.sector_id,retail2_sectors)].groupby('zone_id').employees.sum()
        service2_emp = establishments[np.in1d(establishments.sector_id,service2_sectors)].groupby('zone_id').employees.sum()
        
        tm_export = pd.DataFrame(index=zones.index)
        tm_export['HH_Pop'] = hh_pop
        tm_export['Low_Inc_HH'] = low_inc_hh
        tm_export['Med_Inc_HH'] = med_inc_hh
        tm_export['High_Inc_HH'] = high_inc_hh
        tm_export['TOT_HH'] = tot_hh
        tm_export['AVG_HH_Size'] = avg_hh_size
        tm_export['Prod_Dist_Emp'] = prod_dist_emp
        tm_export['Retail_Emp'] = retail_emp
        tm_export['Service_Emp'] = service_emp
        tm_export['TotalEmployment'] = total_employment
        tm_export['Education_25'] = education_emp
        tm_export['Entertainment_25'] = entertainment_emp
        tm_export['Production_25'] = production_emp
        tm_export['restaurant_25'] = restaurant_emp
        tm_export['Retail_25'] = retail2_emp
        tm_export['Service_25'] = service2_emp

        tm_export = tm_export.fillna(0)

        fixed_vars = pd.read_csv('C:\\urbansim\\data\\travel_model\\fixed.csv')
        variable_vars = pd.read_csv('C:\\urbansim\\data\\travel_model\\variable.csv')

        tm_export = pd.merge(fixed_vars,tm_export,left_on='ZoneID',right_index=True)
        tm_export = pd.merge(tm_export,variable_vars,left_on='ZoneID',right_on='ZoneID')
        
        ##Available parcel coordinates (includes random x,y for big parcels)
        parcel_coords = dset.parcel_coords
        parcel_coords.x = parcel_coords.x.astype('int64')
        parcel_coords.y = parcel_coords.y.astype('int64')
        big_parcels = parcels.index.values[parcels.parcel_sqft>= 435600]
        #####Export jobs table
        e = establishments.reset_index()
        bids = []
        eids = []
        hbs = []
        sids = []
        for idx in e.index:
            for job in range(e.employees[idx]):
                bids.append(e.building_id[idx])
                eids.append(e.index[idx])
                hbs.append(e.home_based_status[idx])
                sids.append(e.sector_id[idx])
        print len(bids)
        print len(eids)
        print len(hbs)
        print len(sids)
        jobs = pd.DataFrame({'job_id':range(1,len(bids)+1),'building_id':bids,'establishment_id':eids,'home_based_status':hbs,'sector_id':sids})
        jobs['parcel_id'] = bpz.parcel_id[jobs.building_id].values
        jobs['urbancenter_id'] = bpz.urbancenter_id[jobs.building_id].values
        jobs['x'] = bpz.centroid_x[jobs.building_id].values.astype('int64')
        jobs['y'] = bpz.centroid_y[jobs.building_id].values.astype('int64')
        jobs['taz05_id'] = bpz.external_zone_id[jobs.building_id].values
        jobs['sector_id_six'] = 1*(jobs.sector_id==61) + 2*(jobs.sector_id==71) + 3*np.in1d(jobs.sector_id,[11,21,22,23,31,32,33,42,48,49]) + 4*np.in1d(jobs.sector_id,[7221,7222,7224]) + 5*np.in1d(jobs.sector_id,[44,45,7211,7212,7213,7223]) + 6*np.in1d(jobs.sector_id,[51,52,53,54,55,56,62,81,92])
        jobs['jobtypename'] = ''
        jobs.jobtypename[jobs.sector_id_six==1] = 'Education'
        jobs.jobtypename[jobs.sector_id_six==2] = 'Entertainment'
        jobs.jobtypename[jobs.sector_id_six==3] = 'Production'
        jobs.jobtypename[jobs.sector_id_six==4] = 'Restaurant'
        jobs.jobtypename[jobs.sector_id_six==5] = 'Retail'
        jobs.jobtypename[jobs.sector_id_six==6] = 'Service'
        big_parcel_ids_with_jobs = np.unique(jobs.parcel_id[np.in1d(jobs.parcel_id,big_parcels)].values)
        for parcel_id in big_parcel_ids_with_jobs:
            idx_jobs_on_parcel = np.in1d(jobs.parcel_id,[parcel_id,])
            coords = parcel_coords[parcel_coords.parcel_id==parcel_id]
            idx_coord = np.random.choice(coords.index,size=idx_jobs_on_parcel.sum(),replace=True)
            x = coords.x.loc[idx_coord].values
            y = coords.y.loc[idx_coord].values
            jobs.x[idx_jobs_on_parcel] = x
            jobs.y[idx_jobs_on_parcel] = y
        del jobs['sector_id_six']
        del jobs['building_id']
        del jobs['establishment_id']
        del jobs['home_based_status']
        del jobs['sector_id']
        jobs.rename(columns={'job_id':'tempid'},inplace=True)
        jobs.to_csv(tm_input_dir+'\\jobs%s.csv'%sim_year,index=False)
        
        conn_string = "host='paris.urbansim.org' dbname='denver' user='drcog' password='M0untains#' port=5433"
        conn = psycopg2.connect(conn_string)
        cursor = conn.cursor()
        
        print 'Loading jobs_xy to db'
        cursor.execute("DROP TABLE IF EXISTS jobs_xy;")
        conn.commit()

        cursor.execute("CREATE TABLE jobs_xy (tempid integer,parcel_id integer,urbancenter_id text,x integer,y integer,taz05_id integer,jobtypename text);")
        conn.commit()

        output = cStringIO.StringIO()
        jobs.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        cursor.copy_from(output, 'jobs_xy', columns =tuple(jobs.columns.values.tolist()))
        conn.commit()
        
        job_buffer_sql = '''
        alter table jobs_xy add column the_geom geometry;
        update jobs_xy set the_geom = ST_SetSRID(ST_MakePoint(x, y), 2232);
        DROP TABLE IF EXISTS job_centroids;
        select taz05_id, avg(x) as x, avg(y) as y into job_centroids from jobs_xy group by taz05_id;
        alter table job_centroids add column the_geom geometry;
        update job_centroids set the_geom = ST_SetSRID(ST_MakePoint(x, y), 2232);
        CREATE INDEX drcog_jobs_geo_idx ON jobs_xy USING GIST (the_geom);
        CREATE INDEX drcog_job_centroids_geo_idx ON job_centroids USING GIST (the_geom);
        DROP TABLE IF EXISTS job_buffer;
        create table job_buffer as(select
            jobs_xy.taz05_id,
            count(*) as empdensempcentroid,
            count(jobtypename = 'Education' or null) as "eddensempcentroid",
            count(jobtypename = 'Entertainment' or null) as "entdensempcentroid",
            count(jobtypename = 'Service' or null) as "servdensempcentroid",
            count(jobtypename = 'Production' or null) as "proddensempcentroid",
            count(jobtypename = 'Restaurant' or null) as "restaurantdensempcentroid",
            count(jobtypename = 'Retail' or null) as "retaildensempcentroid"
        from
            job_centroids
            inner join
            jobs_xy on st_dwithin(job_centroids.the_geom, jobs_xy.the_geom, 2640)
        group by jobs_xy.taz05_id);
        DROP TABLE IF EXISTS job_hh_buffer;
        create table job_hh_buffer as(select
            jobs_xy.taz05_id,
            count(jobtypename = 'Service' or null) as "servdenshhcentroid",
            count(jobtypename = 'Restaurant' or null) as "restaurantemploymenthouseholdbuffer",
            count(jobtypename = 'Retail' or null) as "retailemploymenthouseholdbuffer"
        from
            hh_centroids
            inner join
            jobs_xy on st_dwithin(hh_centroids.the_geom, jobs_xy.the_geom, 2640)
        group by jobs_xy.taz05_id);
        '''
        cursor.execute(job_buffer_sql)
        conn.commit()
        

        #####Export household points
        hh = households[['building_id']].reset_index()
        hh['parcel_id'] = bpz.parcel_id[hh.building_id].values
        hh['urbancenter_id'] = bpz.urbancenter_id[hh.building_id].values
        hh['x'] = bpz.centroid_x[hh.building_id].values
        hh['y'] = bpz.centroid_y[hh.building_id].values
        hh['taz05_id'] = bpz.external_zone_id[hh.building_id].values
        hh['dist_trans'] = np.minimum(bpz.dist_rail[hh.building_id].values, bpz.dist_bus[hh.building_id].values)/5280.0
        big_parcel_ids_with_hh = np.unique(hh.parcel_id[np.in1d(hh.parcel_id,big_parcels)].values)
        for parcel_id in big_parcel_ids_with_hh:
            idx_hh_on_parcel = np.in1d(hh.parcel_id,[parcel_id,])
            coords = parcel_coords[parcel_coords.parcel_id==parcel_id]
            idx_coord = np.random.choice(coords.index,size=idx_hh_on_parcel.sum(),replace=True)
            x = coords.x.loc[idx_coord].values
            y = coords.y.loc[idx_coord].values
            hh.x[idx_hh_on_parcel] = x
            hh.y[idx_hh_on_parcel] = y
        del hh['building_id']
        hh.rename(columns={'index':'tempid'},inplace=True)
        hh.to_csv(tm_input_dir+'\\households%s.csv'%sim_year,index=False)
        
        print 'Loading hh_xy to db'
        cursor.execute("DROP TABLE IF EXISTS hh_xy;")
        conn.commit()

        cursor.execute("CREATE TABLE hh_xy (tempid integer,parcel_id integer,urbancenter_id text,x integer,y integer,taz05_id integer,dist_trans numeric);")
        conn.commit()

        output = cStringIO.StringIO()
        hh.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        cursor.copy_from(output, 'hh_xy', columns =tuple(hh.columns.values.tolist()))
        conn.commit()
        
        hh_buffer_sql = '''
        alter table hh_xy add column the_geom geometry;
        update hh_xy set the_geom = ST_SetSRID(ST_MakePoint(x, y), 2232);
        DROP TABLE IF EXISTS hh_centroids;
        select taz05_id, avg(x) as x, avg(y) as y into hh_centroids from hh_xy group by taz05_id;
        alter table hh_centroids add column the_geom geometry;
        update hh_centroids set the_geom = ST_SetSRID(ST_MakePoint(x, y), 2232);
        CREATE INDEX drcog_hh_geo_idx ON hh_xy USING GIST (the_geom);
        CREATE INDEX drcog_hh_centroids_geo_idx ON hh_centroids USING GIST (the_geom);
        DROP TABLE IF EXISTS hh_buffer;
        create table hh_buffer as(SELECT hh_xy.taz05_id, count(*) as hhvirhhbuffer
        FROM hh_centroids INNER JOIN hh_xy 
        ON ST_DWithin(hh_centroids.the_geom, hh_xy.the_geom, 2640)
        group by hh_xy.taz05_id);
        DROP TABLE IF EXISTS hh_job_buffer;
        create table hh_job_buffer as(SELECT hh_xy.taz05_id, count(*) as hhvirempbuffer
        FROM job_centroids INNER JOIN hh_xy 
        ON ST_DWithin(job_centroids.the_geom, hh_xy.the_geom, 2640)
        group by hh_xy.taz05_id);
        '''
        cursor.execute(hh_buffer_sql)
        conn.commit()
        
        ##Incorporate buffer variables into the taz export
        
        hh_buffer = sql.read_frame('select * from hh_buffer',conn)
        hh_job_buffer = sql.read_frame('select * from hh_job_buffer',conn)
        job_buffer = sql.read_frame('select * from job_buffer',conn)
        job_hh_buffer = sql.read_frame('select * from job_hh_buffer',conn)
        
        hh_buffer = hh_buffer.set_index('taz05_id')
        hh_job_buffer = hh_job_buffer.set_index('taz05_id')
        job_buffer = job_buffer.set_index('taz05_id')
        job_hh_buffer = job_hh_buffer.set_index('taz05_id')
        
        taz = fixed_vars[['TAZ05_ID']]
        taz.columns = ['taz05_id']
        taz = taz.set_index('taz05_id')
        for df in [hh_buffer, hh_job_buffer, job_buffer, job_hh_buffer]:
            for column in df.columns:
                taz[column] = df[column]
        taz = taz.fillna(0)
        taz = taz.rename(columns={'hhvirhhbuffer': 'HouseholdsVirtualHHCentroidBuffer', 'hhvirempbuffer': 'HouseholdsEmpVirtualCentroidBuffer', 'empdensempcentroid': 'EmpDensEmpCentroid', 
                                  'eddensempcentroid': 'EdDensEmpCentroid', 'entdensempcentroid': 'EntDensEmpCentroid', 'servdensempcentroid': 'ServDensEmpCentroid', 
                                  'proddensempcentroid': 'ProdDensEmpCentroid', 'restaurantdensempcentroid': 'RestaurantDensEmpCentroid', 'retaildensempcentroid': 'RetailDensEmpCentroid', 
                                  'restaurantemploymenthouseholdbuffer': 'RestaurantEmploymentHouseholdBuffer', 'retailemploymenthouseholdbuffer': 'RetailEmploymentHouseholdBuffer',
                                  'servdenshhcentroid': 'ServDensHHCentroid', })
        tm_export = pd.merge(tm_export,taz,left_on='TAZ05_ID',right_index=True)
        tm_export['resden'] = tm_export.HouseholdsVirtualHHCentroidBuffer/1000.0
        tm_export['retden'] = (tm_export.RestaurantEmploymentHouseholdBuffer + tm_export.RetailEmploymentHouseholdBuffer)/1000.0
        tm_export['MixedUseDensityHouseholdCentroid'] = (tm_export.retden*tm_export.resden)/np.maximum(np.array([.0001]*2804),(tm_export.retden+tm_export.resden))
        tm_export['resden'] = tm_export.HouseholdsEmpVirtualCentroidBuffer/1000.0
        tm_export['retden'] = (tm_export.RestaurantDensEmpCentroid + tm_export.RetailDensEmpCentroid)/1000.0
        tm_export['MixedUseDensityEmploymentCentroid'] = (tm_export.retden*tm_export.resden)/np.maximum(np.array([.0001]*2804),(tm_export.retden+tm_export.resden))
        del tm_export['resden']
        del tm_export['retden']
        tm_export.to_csv(tm_input_dir+'\\ZonalDataTemplate%s.csv'%sim_year,index=False)
        
        #####Export synthetic households
        h = households[['age_of_head','building_id','cars','children','county','income','income_group_id','persons','race_id','tenure','workers']]
        h.to_csv(tm_input_dir+'\\SynHH%s.csv'%sim_year,index=False)
        
        
        #####################
        #####Other indicators
        #####################
        other_zonal_indicators = pd.DataFrame(index=zones.index)
        other_county_indicators = pd.DataFrame(index=np.unique(parcels.county_id))
        
        ##Number of children in zone
        children = households.groupby('zone_id').children.sum()
        
        ##Median household income in zone
        med_hh_inc = households.groupby('zone_id').income.median()
        
        ##Average household income in zone
        avg_hh_inc = households.groupby('zone_id').income.mean()
        
        ##Number of hh workers by zone
        hh_workers = households.groupby('zone_id').workers.sum()
        
        ##Number of cars in county
        cars = households.groupby('county_id').cars.sum()
        
        ##Employment by county
        emp_by_county = establishments.groupby('county_id').employees.sum()
        
        ##Agricultural jobs in county
        ag_jobs = establishments[establishments.sector_id==11].groupby('county_id').employees.sum()
        
        ##Residential units by county
        resunits = buildings.groupby('county_id').residential_units.sum()
        
        other_zonal_indicators['children'] = children
        other_zonal_indicators['med_hh_inc'] = med_hh_inc
        other_zonal_indicators['avg_hh_inc'] = avg_hh_inc
        other_zonal_indicators['hhworkers_by_zone'] = hh_workers
        
        other_county_indicators['cars'] = cars
        other_county_indicators['county_employment'] = emp_by_county
        other_county_indicators['agricultural_jobs'] = ag_jobs
        
        other_zonal_indicators.to_csv(tm_input_dir+'\\other_zonal_indicators%s.csv'%sim_year)
        other_county_indicators.to_csv(tm_input_dir+'\\other_county_indicators%s.csv'%sim_year)
        
        
        ####RUN TRAVEL MODEL
        # raw_input("Press Enter when travel model is finished running...")
    