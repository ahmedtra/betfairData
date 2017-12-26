# ----------------------------------------------------------------------------------------------------------------------
# IMPORT FOOTBALL DATA : Team, Goals, Odds for Bookmakers into Database
# ----------------------------------------------------------------------------------------------------------------------

import os
import pandas as pd
import pandas.io.sql as pd_sql
import sqlalchemy as sa
import time

import zipfile
from io import StringIO
import numpy as np
import queue
import threading
import datetime
#pymysql.install_as_MySQLdb()
import re


# ----------------------------------------------------------------------------------------------------------------------
# Parse Date to transform in correct format for SQL Database
# ----------------------------------------------------------------------------------------------------------------------

def date_parser(date):
    try:
        return pd.to_datetime(date, format='%d/%m/%y')
    except Exception:
        return pd.to_datetime(date, format='%d/%m/%Y')


# ----------------------------------------------------------------------------------------------------------------------
# Build Dataframe for SQL Database
# ----------------------------------------------------------------------------------------------------------------------

def create_data_frame(data,country,competition):
    print(data)
    df_reader = pd.read_csv(data, header=0, parse_dates=["Date"], date_parser=date_parser, error_bad_lines=False, chunksize=10e4, encoding="ISO-8859-1")
    j = 0
    t = time.time()

    if country != 'Turkey':  # Issue with Turkish Data BSA column not double but string (see below) -> TO FIX
        for df in df_reader:
            j += 1
            df_loop = df.copy()

            # Remove first column in the csv line
            if 'Div' in df_loop.columns:
                df_loop = df_loop.drop(['Div'], axis=1)

            # Remove extra column (Unnamed Column) in csv line
            # Transform Column Header HT->HomeTeam & AT ->AwayTeam(old csv format)
            for c in df_loop.columns:
                if 'Unnamed' in c:
                    df_loop = df_loop.drop([c], axis=1)
                if c == 'HT':
                    df_loop = df_loop.rename(columns={'HT': 'HomeTeam', 'AT': 'AwayTeam'})

            # df_loop = df_loop.dropna(axis = 1, how = 'all')

            # Add New Columns from CSV to Database: Country and Competition
            df_loop.insert(1, 'Country', country)
            df_loop.insert(2, 'Competition', competition)

            df_loop['Date'] = df_loop['Date'].dt.strftime("%Y-%m-%d")

            # Remove empty lines (reference is HomeTeam)
            df_loop = df_loop[df_loop['HomeTeam'].isnull()==False]

            # Remove extra comma from Referee Name (issue in CSV file export later)
            if 'Referee' in df_loop.columns:
                print(df_loop)
                df_loop['Referee'] = df_loop['Referee'].astype(str).apply(lambda x: x.replace(',', ''))

            # Problem with Turkey BSA column not double but string...
            # if country == 'Turkey':
            #     print(df_loop.iloc[0])
            #     try:
            #         # df_loop['BSA'] = df_loop['BSA'].astype('float')
            #         pd.to_numeric(df_loop['BSA'],errors='coerce')
            #     except Exception:
            #         pass
            #     df_loop.to_clipboard()

            elapsed = time.time() - t
            print(data + ' took: ' + str(elapsed))
            write_to_sql(df_loop)


# ----------------------------------------------------------------------------------------------------------------------
# Push Data in SQL Database Table: Football_data_co_uk
# ----------------------------------------------------------------------------------------------------------------------

def write_to_sql(dataframe, tablename='Football_data_co_uk'):
    pd_sql.to_sql(dataframe, tablename, conn, if_exists='append',index=False)


# ----------------------------------------------------------------------------------------------------------------------
# Connect to SQL Database Table: Football_data_co_uk
# Print import Status
# ----------------------------------------------------------------------------------------------------------------------

  if __name__ == "__main__":
    conn = sa.create_engine("mysql://root:Betfair@localhost/betfair?host=localhost?port=3306", convert_unicode=True)
    files_list = []

    for subdir, dirs, files in os.walk('E:\\Football_data.co.uk\\'):
        print(files)
        for f in files:
            files_list.append('E:\\Football_data.co.uk\\'+f)
            print(f)
            country = f.split('_')[0]
            competition = f.split('_')[1]
            print('country: '+country)
            print('competition: '+competition)
            data = 'E:\\Football_data.co.uk\\'+f
            create_data_frame(data, country, competition)
