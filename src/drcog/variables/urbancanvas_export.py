import pandas as pd, numpy as np
import pandas.io.sql as sql
import psycopg2
import cStringIO

def export_to_urbancanvas(building_df,current_year,urbancanvas_scenario_id):
    import pandas.io.sql as sql
    import psycopg2
    import cStringIO
    conn_string = "host='paris.urbansim.org' dbname='denver' user='drcog' password='M0untains#' port=5433"
    conn=psycopg2.connect(conn_string)
    cur = conn.cursor()
    
    if urbancanvas_scenario_id == 0:
        query = "select nextval('developmentproject_id_seq');"
        nextval = sql.read_frame(query,conn)
        nextval = nextval.values[0][0]
        query = "select max(id)+1 from scenario_project;"
        id = sql.read_frame(query,conn)
        id = id.values[0][0]
        query = "INSERT INTO scenario(id, name) VALUES(%s, 'Run #%s');" % (nextval,nextval)
        cur.execute(query)
        conn.commit()
        query = "INSERT INTO scenario_project(id, scenario, project) VALUES(%s, %s, 1);" % (id,nextval)
        cur.execute(query)
        conn.commit()
        query = "select max(id)+1 from scenario_project;"
        id = sql.read_frame(query,conn)
        id = id.values[0][0]
        query = "INSERT INTO scenario_project(id, scenario, project) VALUES(%s, %s, %s);" % (id,nextval,nextval)
        cur.execute(query)
        conn.commit()
    else:
        nextval = urbancanvas_scenario_id
    nextval_string = '{' + str(nextval) + '}'
    building_df['projects'] = nextval_string
    
    valid_from = '{' + str(current_year) + '-1-1}'
    building_df['valid_from'] = valid_from
    building_df['land_area'] = 0
    building_df['tax_exempt'] = 0
    building_df['srcparc_id'] = '0'
    building_df['building_id'] = building_df.index.values
    #building_df['stories'] = 30 ###For testing!
    del building_df['unit_price_residential']
    del building_df['unit_price_non_residential']
    del building_df['building_sqft_per_job']
    del building_df['base_year_jobs']
    del building_df['non_residential_units']
    del building_df['all_units']
    
    print 'Exporting %s buildings to Urbancanvas database for project %s and year %s.' % (building_df.index.size,nextval,current_year)
    output = cStringIO.StringIO()
    building_df.to_csv(output, sep='\t', header=False, index=False)
    output.seek(0)
    cur.copy_from(output, 'building_footprints', columns =tuple(building_df.columns.values.tolist()))
    conn.commit()
    
    return nextval