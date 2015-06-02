import numpy as np, pandas as pd
import pandas.io.sql as sql
import psycopg2
import cStringIO
from drcog.variables import pums_vars
from synthicity.utils import misc

job=pd.read_csv('C:\urbansim\data/travel_model/2015\Jobs2015.csv')

print job[job.taz05_id==412065].parcel_id