import pandas as pd
import numpy as np
import datetime
import requests
import json
from sqlalchemy import create_engine
from ip2geotools.databases.noncommercial import DbIpCity

# def getLocation(ip: str= '134.36.36.75') -> dict:
#     '''Takes IP and returns a dictionary of ip, city, region and country
#     https://pypi.org/project/ip2geotools/'''
#     res = DbIpCity.get(ip, api_key="free")
#     location_data = {
#         "ip": res.ip_address,
#         "city": res.city,
#         "region": res.region,
#         "country": res.country
#     }
#     return location_data

def serverParams():
    #push my data (42) to xcom and assign key= 'my_key'
    engine = create_engine('postgresql://airflow:airflow@postgres:5432/postgres')
    #engine = create_engine('postgresql://postgres:postgres@postgresdb1.cm8ics8yyktp.eu-west-2.rds.amazonaws.com:5432/postgres')
    #engine= create_engine('redshift+redshift_connector://awsuser:Admin1server1!@redshift-cluster-1.cujtr4wipzwf.eu-west-2.redshift.amazonaws.com:5439/pipeline')

    db= 'postgres'
    schema= 'dev'
    return (engine,db, schema)

def createTable(table:str)-> str:
    if table== 'user':
        query_string= """ CREATE TABLE IF NOT EXISTS dev."dim_users_new"
                            (
                                user_ip_id SERIAL CONSTRAINT users_new_pk PRIMARY KEY,
                                ip text COLLATE pg_catalog."default",
                                city text COLLATE pg_catalog."default",
                                region text COLLATE pg_catalog."default",
                                country text COLLATE pg_catalog."default",
                                latitude text COLLATE pg_catalog."default",
                                longitude text COLLATE pg_catalog."default"
                            )
                        TABLESPACE pg_default;

                        ALTER TABLE IF EXISTS dev."dim_users_new"
                            OWNER to postgres;"""

    elif table== 'host':
        query_string= """CREATE TABLE IF NOT EXISTS dev."dim_hosts"
                            (
                                host_ip_id SERIAL CONSTRAINT host_pk PRIMARY KEY,
                                ip text COLLATE pg_catalog."default",
                                city text COLLATE pg_catalog."default",
                                region text COLLATE pg_catalog."default",
                                country text COLLATE pg_catalog."default",
                                latitude text COLLATE pg_catalog."default",
                                longitude text COLLATE pg_catalog."default"
                            )
                        TABLESPACE pg_default;

                        ALTER TABLE IF EXISTS dev."dim_hosts"
                            OWNER to postgres;
                            """
    elif table== 'response':
        query_string= """CREATE TABLE IF NOT EXISTS dev."dim_response"
                            (
                                response_id int CONSTRAINT of_pk PRIMARY KEY
                            )
                        TABLESPACE pg_default;
                        ALTER TABLE IF EXISTS dev."dim_response"
                            OWNER to postgres;
                            """

    elif table== 'filetype':
        query_string= """CREATE TABLE IF NOT EXISTS dev."dim_filetype"
                            (
                                filetype_id SERIAL CONSTRAINT filetype_pk PRIMARY KEY,
                                file_type text COLLATE pg_catalog."default"
                            )
                        TABLESPACE pg_default;
                        ALTER TABLE IF EXISTS dev."dim_filetype"
                            OWNER to postgres;
                            """

    elif table== 'uri':
        query_string= """CREATE TABLE IF NOT EXISTS dev."dim_uri"
                            (
                                uri_id SERIAL CONSTRAINT uri_pk PRIMARY KEY,
                                uri text COLLATE pg_catalog."default"
                            )
                        TABLESPACE pg_default;
                        ALTER TABLE IF EXISTS dev."dim_uri"
                            OWNER to postgres;
                            """

    elif table== 'referer':
        query_string= """CREATE TABLE IF NOT EXISTS dev."dim_referer"
                            (
                                referer_id SERIAL CONSTRAINT referer_pk PRIMARY KEY,
                                referer text COLLATE pg_catalog."default"
                            )
                        TABLESPACE pg_default;
                        ALTER TABLE IF EXISTS dev."dim_referer"
                            OWNER to postgres;
                            """
    
    elif table== 'os':
        query_string= """CREATE TABLE IF NOT EXISTS dev."dim_os"
                            (
                                os_id SERIAL CONSTRAINT os_pk PRIMARY KEY,
                                os text COLLATE pg_catalog."default"
                            )
                        TABLESPACE pg_default;
                        ALTER TABLE IF EXISTS dev."dim_os"
                            OWNER to postgres;
                            """

    elif table== 'browser':
        query_string= """CREATE TABLE IF NOT EXISTS dev."dim_browser"
                            (
                                browser_id SERIAL CONSTRAINT browser_pk PRIMARY KEY,
                                browser text COLLATE pg_catalog."default"
                            )
                        TABLESPACE pg_default;
                        ALTER TABLE IF EXISTS dev."dim_browser"
                            OWNER to postgres;
                            """

    return query_string

def get_location(ip_address='80.210.80.41'):
    response = requests.get('https://geolocation-db.com/jsonp/' + ip_address)
    result = response.content.decode()
    result = result.split("(")[1].strip(")")
    result  = json.loads(result)

    location_data = {
            "ip": ip_address,
            "city": result["city"],
            "region": result["state"],
            "country": result["country_name"],
            "latitude": result["latitude"],
            "longitude": result["longitude"]
        }
    return location_data


def getUniqueFileExtensions(uri_stem_col:pd.Series) -> pd.DataFrame:
    '''Takes a pd.Series column of urls being accessed, and returns a dataframe containing the unique filetypes in the input '''
    split_uri_stream= uri_stem_col.str.split('.')
    file_extensions= []
    for uri in split_uri_stream:
        if uri[-1] in file_extensions:
            pass
        else:
            file_extensions.append(uri[-1])
    file_extensions_df= pd.DataFrame(file_extensions, columns =['file_type'])
    return (file_extensions_df)


def filterNewDimKeys(source_unique_col:pd.Series, dest_compared_col:pd.Series, dim_col_name:str) -> tuple:
    '''takes the source unique column, destination column to be compared and the expected output dimension column name '''
    new_records_detail= pd.DataFrame(columns=[dim_col_name])
    new_records_counter= 0
    escaped_records_counter= 0

    for record in source_unique_col:
        if record is not None:
            if record in dest_compared_col.values:
                escaped_records_counter+= 1
                pass
            else:
                new_records = pd.DataFrame({dim_col_name:record}, index=[0])
                new_records_detail= pd.concat([new_records_detail, new_records], ignore_index=True)
                new_records_counter+= 1
        else:
            new_records = pd.DataFrame({dim_col_name: 0}, index=[0])
            new_records_detail= pd.concat([new_records_detail, new_records], ignore_index=True)
            new_records_counter+= 1

    return(new_records_detail, new_records_counter, escaped_records_counter)


def getUniqueReferers(cs_referer_col:pd.Series) -> pd.DataFrame:
    '''Takes a pd.Series column of cs(Referer) being accessed, and returns a dataframe containing the unique referers in the input '''
    referers_list= cs_referer_col.unique()
    split_referers= []
    for referer in referers_list:
        if referer:
           """Grab the first 7 chars 'https://', split the rest at '/' to ge the main page from eg www.website.com/books/. Take the first item in the list and concat to https:// """
           split_referers.append(referer[0:7]+referer[7:].split('/')[0])
           unique_referers= list(set(split_referers))
    unique_referers_df= pd.DataFrame(unique_referers, columns =['referer'])
    return unique_referers_df



def getUniqueOS(cs_user_agent_col:pd.Series) -> pd.DataFrame:
    source_unique_device_detail= cs_user_agent_col.unique()
    split_details= []
    for detail in source_unique_device_detail:
        if detail is not None:
            os_details= detail.split('(')
            if len(os_details)>1:
                os= os_details[1].split(';')
                if os[0]== 'compatible' and os[1].startswith('+MSIE'):
                    if os[2][1:8] not in split_details:    # Dont add if exists in list
                        split_details.append(os[2][1:8])
                else:
                    if os[0].startswith('Windows'):
                        if os[0][0:7] not in split_details: # Dont add if exists in list
                            split_details.append(os[0][0:7])
                    else:
                        if os[0] not in split_details:  # Dont add if exists in list
                            split_details.append(os[0])
            else:
                if os_details not in split_details: # Dont add if exists in list
                    split_details.append(os_details)
        else:
            split_details.append(detail)

    os_df= pd.DataFrame(split_details, columns =['os'])

    return(os_df)



def getUniqueBrowsers(cs_user_agent_col:pd.Series) -> pd.DataFrame:
    source_unique_device_detail= cs_user_agent_col.unique()
    split_details= []
    for detail in source_unique_device_detail:
        if detail is not None:
            browser= detail.split(')')[-1]
            # detail= detail.replace('+', ' ')
            if len(browser)>1 and browser[0]== '+':
                browser= browser[1:].split('+')[-1]
                if browser is not list:
                    if browser.split('/')[0] not in split_details: # Dont add if exists in list:
                        split_details.append(browser.split('/')[0]) 
                # browser= browser
                else:
                    if browser not in split_details: # Dont add if exists in list:
                        split_details.append(browser) 
            else:
                if browser not in split_details: # Dont add if exists in list:
                    split_details.append(browser)
        else:
            split_details.append(detail)

    browser_df= pd.DataFrame(split_details, columns =['browser'])
    return browser_df


def approxTimeMinute(time_col):
    new_time_detail= pd.DataFrame(columns=['time_c', 'time_id'])
    for time in time_col:
        time_id= str(time)[0:5]
        if time not in new_time_detail['time_c'].values:
            new_time = pd.DataFrame({'time_c':time, 'time_id': time_id}, index=[0])
            new_time_detail= pd.concat([new_time_detail, new_time], ignore_index=True)
    return(new_time_detail)


def rgetFileExtensions(uri_stem_col:pd.Series) -> pd.DataFrame:
    '''Takes a pd.Series column of urls being accessed, and returns a dataframe containing the filetypes of each row of the input '''
    split_uri_stream= uri_stem_col.str.split('.')
    file_extensions= []
    for uri in split_uri_stream:
        file_extensions.append(uri[-1])
    file_extensions_df= pd.DataFrame(file_extensions, columns =['file_type'])
    return file_extensions_df

def rgetReferers(cs_referer_col:pd.Series) -> pd.DataFrame:
    '''Takes a pd.Series column of cs(Referer) being accessed, and returns a dataframe containing the referer for each role in the input '''
    split_referers= []
    for referer in cs_referer_col:
        if referer:
           """Grab the first 7 chars 'https://', split the rest at '/' to ge the main page from eg www.website.com/books/. Take the first item in the list and concat to https:// """
           split_referers.append(referer[0:7]+referer[7:].split('/')[0])
          # unique_referers= list(set(split_referers))
        else: 
            split_referers.append(np.nan)
    split_referers_df= pd.DataFrame(split_referers, columns =['referer'])
    return split_referers_df


def rgetOS(cs_user_agent_col:pd.Series) -> pd.DataFrame:
    split_details= []
    for detail in cs_user_agent_col:
        if detail is not None:
            os_details= detail.split('(')
            if len(os_details)>1:
                os= os_details[1].split(';')
                if os[0]== 'compatible' and os[1].startswith('+MSIE'):
                    split_details.append(os[2][1:8])
                else:
                    if os[0].startswith('Windows'):
                        split_details.append(os[0][0:7])
                    else:
                        split_details.append(os[0])
            else:
                split_details.append(os_details)
        else:
            split_details.append(detail)
    os_df= pd.DataFrame(split_details, columns =['os'])

    return(os_df)

def rgetBrowsers(cs_user_agent_col:pd.Series) -> pd.DataFrame:
    split_details= []
    for detail in cs_user_agent_col:
        if detail is not None:
            browser= detail.split(')')[-1]
            # detail= detail.replace('+', ' ')
            if len(browser)>1 and browser[0]== '+':
                browser= browser[1:].split('+')[-1]
                if browser is not list:
                    split_details.append(browser.split('/')[0]) 
                # browser= browser
                else:
                    split_details.append(browser) 
            else:
                split_details.append(browser)
        else:
            split_details.append(detail)
    browser_df= pd.DataFrame(split_details, columns =['browser'])
    return browser_df