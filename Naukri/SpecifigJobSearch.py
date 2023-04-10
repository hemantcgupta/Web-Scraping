# -*- coding: utf-8 -*-
"""
Created on Sun Apr  2 00:07:55 2023

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
import warnings
warnings.filterwarnings('ignore')
pd.options.display.max_columns = 4
options = Options()
options.add_argument('--headless')
options.add_argument('--disable-gpu')
options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36")
options.add_argument('--disable-javascript')
# =============================================================================
# URLS Continers
# =============================================================================
url = "https://www.naukri.com/python-machine-learning-jobs-in-navi-mumbai?k=python%2C%20machine%20learning&l=navi%20mumbai%2C%20mumbai&experience=2&nignbevent_src=jobsearchDeskGNB"
# https://www.naukri.com/python-machine-learning-jobs?k=python%2C%20machine%20learning&experience=0&nignbevent_src=jobsearchDeskGNB
# =============================================================================
# Setting selenium and fetch the body then convert htmlsparser and fetch jub_tuples
# =============================================================================
driver = webdriver.Chrome(options=options)
driver.get(url)
# time.sleep(1) 
body_content = driver.execute_script("return document.body.innerHTML")
# Converting the into html Parser
soup = BeautifulSoup(body_content, 'html.parser')
job_tuples = soup.find_all('article', class_='jobTuple')
len(job_tuples)
# =============================================================================
# Data Fething and formating 
# =============================================================================
def JobApplicants(title_href):
    driver.get(title_href)
    body_content = driver.execute_script("return document.body.innerHTML")
    # Converting the into html Parser
    soup = BeautifulSoup(body_content, 'html.parser')
    try:
        job_applicants = soup.find_all('span', {'class': 'stat'})[-1].text.split(': ')[-1]
        return job_applicants
    except:
        print(title_href)
        return 'Error'
def MainDataFetch(job_tuple):
    dct = dict()
    # Job_Id
    job_id = job_tuple['data-job-id']
    # Job title and Job Link
    title = job_tuple.find('a', class_='title ellipsis')
    title_text = title.text
    title_href = title['href']
    Job_Applicants = JobApplicants(title_href)
    # Company Name and Link
    companyInfo = job_tuple.find('a', class_='subTitle ellipsis fleft') 
    companyInfo_text = companyInfo.text
    companyInfo_href = companyInfo['href']
    # Required Experince
    experience = job_tuple.find('span', {'class': 'ellipsis fleft expwdth'}).text
    # Offered Salary
    salary = job_tuple.find('span', {'class': 'ellipsis fleft'}).text
    # Job Location
    Location = job_tuple.find('span', {'class': 'ellipsis fleft locWdth'}).text
    # Job Psot Time
    post_time = job_tuple.find('span', {'class': 'fleft postedDate'}).text
    # Stored the all Data into Dictionary adn then append into the list
    dct['ScrappingTime'] = date.today().strftime('%Y-%m-%d')
    dct['Job_Id'] = job_id
    dct['Job_Title'] = title_text
    dct['Job_Link'] = title_href
    dct['Compnay_Name'] = companyInfo_text
    dct['Coompany_Link'] = companyInfo_href
    dct['Experince_Level'] = experience
    dct['Offered_Salary'] = salary
    dct['Job_Location'] = Location
    dct['Job_PostTime'] = post_time
    dct['Job_Applicants'] = Job_Applicants
    return dct   
df_lst = pd.DataFrame([MainDataFetch(job_tuple) for job_tuple in job_tuples])
df_lst['DaysAga'] = df_lst['Job_PostTime'].str.extract('(\d+)').replace(np.nan, 0).astype(int)
df_lst['PostDate'] = (datetime.today() - pd.to_timedelta(df_lst['DaysAga'], unit='d')).dt.date

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

def checkduplicates(df_lst):
    df_sql = pd.read_sql('''select * from JobSearch''', cnxn())
    df_sql = df_sql[~df_sql['Job_Id'].isin(df_lst['Job_Id'])]
    return pd.concat([df_sql, df_lst])
    
Data_Inserting_Into_DB(checkduplicates(df_lst), 'JobSearch')



