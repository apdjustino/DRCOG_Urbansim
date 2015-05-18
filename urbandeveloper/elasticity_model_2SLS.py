__author__ = 'JMartinez'


import numpy as np, pandas as pd, os
from synthicity.utils import misc
import pysal as py

class elasticity_model(object):


    def __init__(self, dset):
        self.zones = dset.zones
        self.buildings_far = pd.merge(dset.buildings, dset.fars, left_on='far_id', right_index=True)




    def estimate_elasticity(self, zones):
        dummies = pd.get_dummies(zones.county)
        zones = pd.concat([zones, dummies], axis=1)
        zones['avg_far'] = self.buildings_far.groupby('zone_id').far.mean() #use far_x because Xavier's code adds far to buildings

        #zones = zones[zones.residential_sqft_zone>0]

        #wrook = py.queen_from_shapefile('C:/users/jmartinez/documents/Test Zones/zones_prj_res2.shp')
        wqueen = py.queen_from_shapefile(os.path.join(misc.data_dir(),'shapefiles\\zones.shp'))
        w = py.weights.weights.W(wqueen.neighbors, wqueen.weights)
        x = zones[['zonal_pop','mean_income']]
        x = x.apply(np.log1p)

        x['ln_jobs_within_30min'] = zones['ln_jobs_within_30min']
        x['zone_contains_park'] = zones['zone_contains_park']
        x['Arapahoe'] = zones['Arapahoe']
        x['Boulder'] = zones['Boulder']
        x['Broomfield'] = zones['Broomfield']
        x['Clear Creek'] = zones['Clear Creek']
        x['Denver'] = zones['Denver']
        x['Douglas'] = zones['Douglas']
        x['Elbert'] = zones['Elbert']
        x['Gilpin'] = zones['Gilpin']
        x['Jefferson'] = zones['Jefferson']
        x['Weld'] = zones['Weld']
        x=x.fillna(0)
        x = x.as_matrix()

        imat = zones[['ln_avg_nonres_unit_price_zone','avg_far']]
        imat = imat.fillna(0)
        imat = imat.as_matrix()

        yend = zones['ln_avg_unit_price_zone']
        yend = yend.fillna(0)
        yend = yend.as_matrix()
        yend = np.reshape(yend,(zones.shape[0],1))

        y = zones['residential_sqft_zone']
        y = y.fillna(0)
        y = y.apply(np.log1p)
        y = y.as_matrix()
        y = np.reshape(y,(zones.shape[0],1))


        imat_names = ['non_res_price','avg_far']
        x_names = ['zonal_pop', 'mean_income', 'ln_jobs_within_30min', 'zone_contains_park','Arapahoe','Boulder','Broomfield','Clear Creek','Denver','Douglas','Elbert','Gilpin','Jefferson','Weld']
        yend_name = ['ln_avg_unit_price_zone']
        y_name = 'residential_sqft_zone'

        reg_2sls = py.spreg.twosls_sp.GM_Lag(y, x, yend=yend, q=imat, w=w, w_lags=2, robust ='white', name_x = x_names, name_q = imat_names, name_y = y_name, name_yend = yend_name)

        demand_elasticity = np.absolute(reg_2sls.betas[15])
        demand_elasticity = 1/demand_elasticity[0]
        #
        return demand_elasticity


    def estimate_non_res_elasticity(self,zones):

        dummies = pd.get_dummies(zones.county)
        zones = pd.concat([zones, dummies], axis=1)
        zones['avg_far'] = self.buildings_far.groupby('zone_id').far.mean() #use far_x because Xavier's code adds far to buildings
        #zones = zones[zones.non_residential_sqft_zone>0]

        ####spatial weights matrix#####
        #zones = zones.reset_index()
        #zone_coord = zones[['zone_id','zonecentroid_x', 'zonecentroid_y']]

        #zone_coord = zone_coord.as_matrix()

        wqueen = py.queen_from_shapefile(os.path.join(misc.data_dir(),'shapefiles\\zones.shp'))
        #w = py.weights.Distance.DistanceBand(zone_coord, threshold = 50000, binary = False)
        #w.transform ='r'
        #w = py.weights.weights.W(w.neighbors, w.weights)
        w = py.weights.weights.W(wqueen.neighbors, wqueen.weights)
        x = zones[['zonal_emp','residential_units_zone']]
        x = x.apply(np.log1p)
        #x['ln_emp_aggsector_within_5min'] = zones['ln_emp_aggsector_within_5min']
        #x['zone_contains_park'] = zones['zone_contains_park']
        x['percent_younghead'] = zones['percent_younghead']
        x['Arapahoe'] = zones['Arapahoe']
        x['Boulder'] = zones['Boulder']
        x['Broomfield'] = zones['Broomfield']
        x['Clear Creek'] = zones['Clear Creek']
        x['Denver'] = zones['Denver']
        x['Douglas'] = zones['Douglas']
        x['Elbert'] = zones['Elbert']
        x['Gilpin'] = zones['Gilpin']
        x['Jefferson'] = zones['Jefferson']
        x['Weld'] = zones['Weld']
        x=x.fillna(0)
        x = x.as_matrix()

        imat = zones[['ln_avg_unit_price_zone','avg_far']]
        imat = imat.fillna(0)
        imat = imat.as_matrix()

        yend = zones['ln_avg_nonres_unit_price_zone']
        yend = yend.fillna(0)
        yend = yend.as_matrix()
        yend = np.reshape(yend,(zones.shape[0],1))

        y = zones['non_residential_sqft_zone']
        y = y.fillna(0)
        y = y.apply(np.log1p)
        y = y.as_matrix()
        y = np.reshape(y,(zones.shape[0],1))


        imat_names = ['res_price','avg_far']
        x_names = ['zonal_emp', 'residential_units_zone', 'percent_younghead','Arapahoe','Boulder','Broomfield','Clear Creek', 'Denver', 'Douglas','Elbert','Gilpin','Jefferson','Weld']
        yend_name = ['ln_avg_nonres_unit_price_zone']
        y_name = 'non_residential_sqft_zone'

        reg_2sls = py.spreg.twosls_sp.GM_Lag(y, x, yend=yend, q=imat, w=w, w_lags=2,robust ='white', name_x = x_names, name_q = imat_names, name_y = y_name, name_yend = yend_name)

        #
        # ######estimation
        # x = zones[['zonal_emp','residential_units_zone']]
        # x = x.apply(np.log1p)
        # #x['ln_emp_aggsector_within_5min'] = zones['ln_emp_aggsector_within_5min']
        # #x['zone_contains_park'] = zones['zone_contains_park']
        # x['percent_younghead'] = zones['percent_younghead']
        # x=x.fillna(0)
        # x = x.as_matrix()
        #
        # imat = zones[['ln_avg_unit_price_zone','ln_avg_land_value_per_sqft_zone','median_year_built']]
        # imat = imat.fillna(0)
        # imat = imat.as_matrix()
        #
        # yend = zones['ln_avg_nonres_unit_price_zone']
        # yend = yend.fillna(0)
        # yend = yend.as_matrix()
        # yend = np.reshape(yend,(zones.shape[0],1))
        #
        # y = zones['non_residential_sqft_zone']
        # y = y.fillna(0)
        # y = y.apply(np.log1p)
        # y = y.as_matrix()
        # y = np.reshape(y,(zones.shape[0],1))
        #
        #
        # imat_names = ['res_price','land_value','median_year_built']
        # x_names = ['zonal_emp', 'residential_units_zone', 'percent_younghead']
        # yend_name = ['ln_avg_nonres_unit_price_zone']
        # y_name = 'non_residential_sqft_zone'
        #
        # reg_2sls = py.spreg.twosls_sp.GM_Lag(y, x, yend=yend, q=imat, w=w, robust ='white', name_x = x_names, name_q = imat_names, name_y = y_name, name_yend = yend_name)
        #
        #
        demand_elasticity = np.absolute(reg_2sls.betas[14])
        demand_elasticity = 1/demand_elasticity[0]
        #
        return demand_elasticity
