# -*- coding: utf-8 -*-
"""
Created on Fri Apr  7 13:37:18 2023

@author: Hemant
"""

# =============================================================================
# Main Page Fetch
# =============================================================================
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
import re
import warnings
warnings.filterwarnings('ignore')
pd.options.display.max_columns = 4
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
options.add_argument('--disable-javascript')

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

# =============================================================================
# Data Fething and formating 
# =============================================================================
def KMB(rev):
    if 'k' in rev.lower(): 
        return int(float(rev[:-1])*1000)
    elif 'm' in rev.lower():
        return int(float(rev[:-1])*1000000)
    elif 'b' in rev.lower():
        return int(float(rev[:-1])*1000000000)
    else:
        return int(rev)
    
def MainDataFetch(company_tuple):
    dct = dict()
    CMP_id = company_tuple['id']
    title = company_tuple.find('a', class_='titleAnchor')
    title_text = title.text
    title_href = 'https://www.naukri.com'+title['href']
    Reviews = re.search(r'\d+(\.\d+)?.', company_tuple.find('span', {'class': 'main-2 reviews'}).text).group()
    dct['Cmp_Id'] = CMP_id
    dct['Cmp_Name'] = title_text
    dct['Cmp_Link'] = title_href
    dct['Reviews'] = KMB(Reviews)
    return dct


# =============================================================================
# URLS Continers
# =============================================================================
def FetchMultiPage(options):   
    url = "https://www.naukri.com/companies-hiring-in-india?src=mnjCompanies_homepage_srch&title=Top+companies&subtitle=Hiring+for+Data+Science+%26+Machine+Learning&searchType=companySearch&qcallRoleCategory=1019&qcallDept=3&qccustomTag=195"
    # =============================================================================
    # Setting selenium and fetch the body then convert htmlsparser and fetch jub_tuples
    # =============================================================================
    driver = webdriver.Chrome(options=options)
    driver.get(url)
    # time.sleep(1) 
    body_content = driver.execute_script("return document.body.innerHTML")
    # Converting the into html Parser
    soup = BeautifulSoup(body_content, 'html.parser')
    company_tuples = soup.find_all('div', class_='freeTuple')
    len(company_tuples)
    return pd.DataFrame([MainDataFetch(company_tuple) for company_tuple in company_tuples])

df_lst = FetchMultiPage(options)

def checkduplicates(df_lst):
    df_sql = pd.read_sql('''select * from Companies''', cnxn())
    df_sql.drop_duplicates(subset=['Cmp_Id'], inplace=True)
    df_sql = df_sql[~df_sql['Cmp_Id'].isin(df_lst['Cmp_Id'])]
    return pd.concat([df_sql, df_lst])
    
Data_Inserting_Into_DB(checkduplicates(df_lst), 'Companies')



