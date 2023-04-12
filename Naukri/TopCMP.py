# -*- coding: utf-8 -*-
"""
Created on Wed Apr 12 12:49:00 2023

@author: Hemant
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import webbrowser
import time
import pyautogui
import requests
from bs4 import BeautifulSoup
import pyperclip
import pandas as pd
import numpy as np
import pyodbc
import sqlalchemy
import urllib.request
from datetime import date
from datetime import datetime, timedelta
import warnings
from tqdm import tqdm
import re
warnings.filterwarnings('ignore')


# =============================================================================
# Instert data Into Database
# =============================================================================
def cnxn():
    cnxn_str = ('''DRIVER={ODBC Driver 17 for SQL Server};
                SERVER=DESKTOP-TCVU2DI\LOCALHOST;
                DATABASE=Naukri;
                UID=sa;
                PWD=bluebird''')
    return pyodbc.connect(cnxn_str)

def Data_Inserting_Into_DB(df, Table_Name):
    params = urllib.parse.quote_plus('''DRIVER={ODBC Driver 17 for SQL Server};
                                        SERVER=DESKTOP-TCVU2DI\LOCALHOST;
                                        DATABASE=Naukri;
                                        UID=sa;
                                        PWD=bluebird''')   
    engine = sqlalchemy.create_engine('mssql+pyodbc:///?odbc_connect={}'.format(params))
    
    @sqlalchemy.event.listens_for(engine, 'before_cursor_execute')
    def receive_before_cursor_execute(conn, cursor, statement, params, context, executemany):
        if executemany:
            cursor.fast_executemany = True
    
    start = time.time()
    df.to_sql(Table_Name, engine, if_exists = 'replace', index=False)
    
    
    
def Top_CMP(df_lst):
    df_sql = pd.read_sql('''select * from Cmp_Jobs''', cnxn())
    df_sql = df_sql[~df_sql['Job_Id'].isin(df_lst['Job_Id'])]
    return pd.concat([df_sql, df_lst])

df_sql = pd.read_sql('''select * from Companies''', cnxn())  
df_sql.columns
df_sql['Cmp_Name']











