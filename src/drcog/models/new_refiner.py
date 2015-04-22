__author__ = 'JMartinez'
from collections import OrderedDict
import pandas as pd

def add_res_buildings(dset):

    res_buildings = [1421,1866,1960,2470,2680,2698,2699,2707,2709,2712,2716,2722]#list of zones that need new buildings
    for zone in res_buildings:
        new_building_id = dset.buildings.index.values.max() + 1

        try:
            pid = dset.parcels.parcel_id[dset.parcels.zone_id==zone].values[0]
        except:
            pid = add_parcel(dset, zone)

        new_building_dict = OrderedDict() # create dictionary of the columns in the buildings table
        for i in dset.buildings.columns:
            new_building_dict[i]=0

        # fill correct attributes with sample data

        new_building_dict['building_type_id'] = 20
        #new_building_dict['zone_id'] = zone
        new_building_dict['improvement_value'] = 10000
        new_building_dict['land_area'] = 200
        new_building_dict['residential_units'] = 2
        new_building_dict['parcel_id'] = pid
        new_building_dict['year_built'] = 2000
        new_building_dict['bldg_sq_ft'] = 500
        new_building_dict['unit_price_residential'] = 5000
        #new_building_dict['sqft_per_unit'] = 250
        #new_building_dict['non_residential_units'] = 2
        #new_building_dict['all_units'] = 2

        #create list of values from dictionary


        new_building_data = new_building_dict.values()
        dset.buildings.loc[new_building_id] = new_building_data

        print "Successfully added residential building in zone %d" %zone

def add_non_res_buildings(dset):

    non_res_buildings = [1822,1845,1846,1857,1858,2704,2713,2720]

    for zone in non_res_buildings:

        new_building_id = dset.buildings.index.values.max() + 1

        try:
            pid = dset.parcels.parcel_id[dset.parcels.zone_id==zone].values[0]
        except:
            pid = add_parcel(dset, zone)

        new_building_dict = OrderedDict() # create dictionary of the columns in the buildings table
        for i in dset.buildings.columns:
            new_building_dict[i]=0

        new_building_dict['building_type_id'] = 4
        #new_building_dict['zone_id'] = zone
        new_building_dict['improvement_value'] = 10000
        new_building_dict['land_area'] = 200
        new_building_dict['non_residential_sqft'] = 500
        new_building_dict['parcel_id'] = pid
        new_building_dict['year_built'] = 2000
        new_building_dict['bldg_sq_ft'] = 500
        new_building_dict['unit_price_non_residential'] = 2
        #new_building_dict['building_sqft_per_job'] = 250
        #new_building_dict['non_residential_units'] = 2
        #new_building_dict['all_units'] = 2

        #create list of values from dictionary
        new_building_data = new_building_dict.values()
        dset.buildings.loc[new_building_id] = new_building_data

        print "Successfully added non-residential building in zone %d" %zone

def add_parcel(dset,zone_id):

    p = dset.parcels
    z = dset.zones
    county = z.county.values[z.index.values==zone_id][0]
    x = z.zonecentroid_x.values[z.index.values==zone_id][0]
    y = z.zonecentroid_y.values[z.index.values==zone_id][0]
    if county == 'Denver':
        county_id = 8031
    elif county == 'Adams':
        county_id = 8001
    elif county == 'Arapahoe':
        county_id = 8005
    elif county == 'Boulder':
        county_id = 8013
    elif county == 'Broomfield':
        county_id = 8014
    elif county == 'Clear Creek':
        county_id = 8019
    elif county == 'Douglas':
        county_id = 8035
    elif county == 'Elbert':
        county_id = 8039
    elif county == 'Gilpin':
        county_id = 8047
    elif county == 'Jefferson':
        county_id = 8059
    elif county == 'Weld':
        county_id = 8123
    pid = p.index.values.max()+1

    new_parcel_dict = OrderedDict()

    for i in p.columns:
        new_parcel_dict[i] = 0

    new_parcel_id = p.parcel_id.values.max() + 1

    new_parcel_dict['parcel_id'] = new_parcel_id
    new_parcel_dict['county_id'] = county_id
    new_parcel_dict['parcel_sqft'] = 43560
    new_parcel_dict['land_value'] = 0
    new_parcel_dict['zone_id'] = zone_id
    new_parcel_dict['centroid_x'] = x
    new_parcel_dict['centroid_y'] = y
    new_parcel_dict['dist_bus'] = 6000
    new_parcel_dict['dist_rail'] = 6000
    new_parcel_dict['in_ugb'] = 1


    new_parcel_data = new_parcel_dict.values()
    new_parcel_frame = pd.DataFrame(columns=dset.parcels.columns)
    new_parcel_frame.loc[0] = new_parcel_data

    #dset.parcels.loc[pid] = new_parcel_data
    dset.parcels = pd.concat([dset.parcels, new_parcel_frame])

    print "added new parcel with id %d" % new_parcel_id


    return new_parcel_id


if __name__ ==  '__main__':

    from drcog.models import dataset
    dset = dataset.DRCOGDataset('C:\urbansim\data\drcog.h5')
    add_res_buildings(dset)
