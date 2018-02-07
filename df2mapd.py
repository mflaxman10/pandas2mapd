#!/usr/bin/python
 
"""
 Module for connecting to MapD database and creating tables with the provided data.
"""

__author__ = 'veda.shankar@gmail.com (Veda Shankar)'

import argparse
import sys
import string
import csv

import os
import pandas as pd
from pymapd import connect

connection = "NONE"

# Connect to the DB
def connect_to_mapd(str_user, str_password, str_host, str_dbname):
 global connection
 connection = connect(user=str_user, password=str_password, host=str_host, dbname=str_dbname)
 print connection

# Load CSV to DB
def load_to_mapd(table_name, csv_file):
 global connection
 csv_df = pd.read_csv(csv_file)
 print "loading ..."
 create_table_str = 'CREATE TABLE %s (ga_networkLocation TEXT ENCODING DICT(8), ga_city TEXT ENCODING DICT(8), ga_country TEXT ENCODING DICT(8), ga_landingPagePath TEXT ENCODING DICT(8), ga_deviceCategory TEXT ENCODING DICT(8), ga_pageviews SMALLINT)' % (table_name)
 print create_table_str
 connection.execute(create_table_str)
 connection.load_table(table_name, csv_df, preserve_index=False)
 print connection.get_table_details(table_name)

 
def drop_table_mapd(table_name):
 command = 'drop table if exists %s' % (table_name)
 print command
 connection.execute(command)

def fix_date_table(csv_file):
 command = './fixdate.sh %s' % (csv_file)
 print command
 os.system(command)

table_name = "foobar"
table_filename = "./data/foobar.csv"
#connect_to_mapd("username", "password", "hostname<no port#>", "DB name")
drop_table_mapd(table_name)
load_to_mapd(table_name, table_filename)
