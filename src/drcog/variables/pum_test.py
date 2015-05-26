import pandas as pd, numpy as np
import psycopg2
import pandas.io.sql as sql

def get_pums():
    conn_string = "host='paris.urbansim.org' dbname='denver' user='drcog' password='M0untains#' port=5433"
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    pums_hh = sql.read_frame('select * from pums_hh',conn)

get_pums()

