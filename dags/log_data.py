from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
import os 
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime
from helpers.helper import  get_location, serverParams, createTable, filterNewDimKeys, getUniqueFileExtensions, getUniqueReferers, getUniqueOS, getUniqueBrowsers, approxTimeMinute, rgetFileExtensions, rgetReferers, rgetOS, rgetBrowsers

from datetime import datetime
    


def _readLogToStaging(ti):
    '''Read the logfile names from the path into a list, logfiles'''
    # engine = create_engine('postgresql://postgres:******@postgresdb1.cm8ics8yyktp.eu-west-2.rds.amazonaws.com:5432/postgres')
    # db= 'postgres'
    # schema= 'dev'
    engine,db, schema= serverParams()

    print('trying to access folder /opt/airflow/dags/source_logs"')
    filenames= list(os.listdir("/opt/airflow/dags/source_logs"))
    print('successfully accessed folder /opt/airflow/dags/source_logs"')
    logfiles=[]
    for filename in filenames:
        if filename.split('.')[-1]=='log':
            logfile= '/opt/airflow/dags/source_logs/'+filename
            logfiles.append(logfile)
        else: 
            pass

    '''Check if the tracker table exists and import. Create an empty df if does not exit.'''
    try:
        extracted_logs_tracker= pd.read_sql_table('extracted_logs_tracker',con= engine, schema= schema)
    except:
        # extracted_logs_tracker= pd.DataFrame([])
        extracted_logs_tracker= pd.DataFrame(columns=['log_name', 'datetime_executed'])

    '''Read each log file and the corresponding column names from the logfile and append into one file'''
    df_newlogs= pd.DataFrame(columns=['date', 'time', 's-ip', 'cs-method', 'cs-uri-stem', 'cs-uri-query', 's-port', 'cs-username', 'c-ip', 'cs(User-Agent)', 'cs(Cookie)', 'cs(Referer)', 'sc-status', 'sc-substatus', 'sc-win32-status', 'sc-bytes', 'cs-bytes', 'time-taken'])
    # logs_tracker= pd.DataFrame(columns=['log_name'])
    logs_tracker= pd.DataFrame(columns=['log_name', 'datetime_executed'])
    logfile_counter=0
    for logfile in logfiles:
        # if logfile in extracted_logs_tracker.values:
        if logfile in extracted_logs_tracker['log_name'].values:
            print(f'log file {logfile} already extracted')
        else:
            logfile_counter+=1
            with open(logfile, 'r') as f:
                lines= f.readlines()
                col_names=(lines[3][9:])        #get the column names from the log file comment on line 4, starting from character 9
                col_names=col_names.split()
            df_newlog = pd.read_csv(logfile, sep=' ', comment='#',header= None, names=col_names, on_bad_lines='skip')
            df_newlogs= pd.concat([df_newlogs, df_newlog], ignore_index=True)
            # logfile_df = pd.DataFrame({'log_name':logfile}, index=[0])
            logfile_df = pd.DataFrame({'log_name':logfile, 'datetime_executed': datetime.now().strftime('%Y-%m-%d %H:%M:%S')}, index=[0])
            logs_tracker= pd.concat([logs_tracker, logfile_df], ignore_index=True)

    print('writing log data to staging sql table: staging_IIS_log_data')
    df_newlogs.to_sql('staging_IIS_log_data', engine, if_exists='replace', schema= schema, method='multi', index= False)

    print('writing log tracker to sql table')
    logs_tracker.to_sql('extracted_logs_tracker', engine, if_exists='append', schema= schema, method='multi', index= False)
    print('Log read to staging table: '+ db+'/'+schema)



def _updateUserDimension(ti):
    '''User Dimension'''
    engine,db, schema= serverParams()

    try:
        existing_ips= pd.read_sql_table('dim_users_new',con= engine, schema= schema)
    except:
        # existing_ips= pd.DataFrame([])
        with engine.connect() as con:
            print('creating user table')
            con.execute(createTable('user'))
            print('user table created')
        existing_ips= pd.read_sql_table('dim_users_new',con= engine, schema= schema)

    log_data= pd.read_sql_table('staging_IIS_log_data',con= engine, schema= schema)
    unique_ips= log_data['c-ip'].unique()
    # unique_ips= rows_in_df1_not_in_df2['ip']

    new_ips_detail= pd.DataFrame(columns=['ip', 'city', 'region','country', 'latitude','longitude'])
    new_users_counter= 0
    escaped_users_counter= 0

    for ip in unique_ips:
        if ip in existing_ips['ip'].values:
            escaped_users_counter+= 1
            pass
        else:
            if ip is not None:
                if len(ip)<= 5:
                    if '0' in existing_ips['ip'].values:
                        escaped_users_counter+= 1
                        pass
                    else:
                        print('invalid IP ' + ip + ' , setting values to nil ')
                        ip_details= pd.DataFrame({'ip': '0', 'city': 'nil', 'region': 'nil', 'country': 'nil', 'latitude': np.nan,'longitude': np.nan}, index=[0])
                        new_ips_detail= pd.concat([new_ips_detail, ip_details], ignore_index=True)
                        new_users_counter+= 1
                else:
                    #print(ip + ' is ip')
                    
                    ip_details = pd.DataFrame(get_location(ip), index=[0])
                    # print('check out this ip ' + ip)
                    new_ips_detail= pd.concat([new_ips_detail, ip_details], ignore_index=True)
                    new_users_counter+= 1
            else: 
                if 'nil' in existing_ips['ip'].values:
                    escaped_users_counter+= 1
                    pass
                else:
                    #print('Empty IP, setting values to nil ')
                    ip_details= pd.DataFrame({'ip': '0', 'city': 'nil', 'region': 'nil', 'country': 'nil', 'latitude': np.nan,'longitude': np.nan}, index=[0])
                    new_ips_detail= pd.concat([new_ips_detail, ip_details], ignore_index=True)
                    new_users_counter+= 1
    print('old users: '+ str(escaped_users_counter) +', new users: '+ str(new_users_counter)+ ', writng to new ips(if any to dim_user)')
    new_ips_detail.to_sql('dim_users_new', engine, if_exists='append',schema= schema, method='multi', index= False)
    print('Done updating dim_user')

def _updateHostDimension(ti):
    '''Host Dimension'''
    engine,db, schema= serverParams()

    try:
        existing_hosts= pd.read_sql_table('dim_hosts',con= engine, schema= schema)
    except:
        # existing_ips= pd.DataFrame([])
        with engine.connect() as con:
            con.execute(createTable('host'))
        existing_hosts= pd.read_sql_table('dim_hosts',con= engine, schema= schema)

    log_data= pd.read_sql_table('staging_IIS_log_data',con= engine, schema= schema)
    unique_ips= log_data['s-ip'].unique()

    new_hosts_detail= pd.DataFrame(columns=['ip', 'city', 'region','country'])
    new_hosts_counter= 0
    escaped_hosts_counter= 0

    for ip in unique_ips:
        if ip in existing_hosts['ip'].values:
            escaped_hosts_counter+= 1
            pass
        else:
            if ip is not None:
                if len(ip) <= 2:
                    if '80' in existing_hosts['ip'].values:
                        escaped_hosts_counter+= 1
                        pass
                    else:
                        #print('invalid IP ' + ip + ' , setting values to nil ')
                        host_details= pd.DataFrame({'ip': '80', 'city': 'nil', 'region': 'nil', 'country': 'nil'}, index=[0])
                        new_hosts_detail= pd.concat([new_hosts_detail, host_details], ignore_index=True)
                        new_hosts_counter+= 1
                else:
                    host_details = pd.DataFrame(get_location(ip), index=[0])
                    new_hosts_detail= pd.concat([new_hosts_detail, host_details], ignore_index=True)
                    new_hosts_counter+= 1
            else: 
                if 'nil' in existing_hosts['ip'].values:
                    escaped_hosts_counter+= 1
                    pass
                else:
                    #print('Empty IP, setting values to nil ')
                    host_details= pd.DataFrame({'ip': 'nil', 'city': 'nil', 'region': 'nil', 'country': 'nil'}, index=[0])
                    new_hosts_detail= pd.concat([new_hosts_detail, host_details], ignore_index=True)
                    new_hosts_counter+= 1
                
    print('old hosts: '+ str(escaped_hosts_counter) +', new hosts: '+ str(new_hosts_counter)+ ', writng to new host_ips(if any to dim_host)')
    new_hosts_detail.to_sql('dim_hosts', engine, if_exists='append', schema= schema, method='multi', index= False)
    print('Done updating dim_hosts')

def _updateResponseDimension(ti):
    '''Response Dimension'''
    engine,db, schema= serverParams()

    try:
        existing_responses= pd.read_sql_table('dim_response',con= engine, schema= schema)
    except:
        with engine.connect() as con:
            con.execute(createTable('response'))
        existing_responses= pd.read_sql_table('dim_response',con= engine, schema= schema)

    log_data= pd.read_sql_table('staging_IIS_log_data',con= engine, schema= schema)
    unique_response_statuses= np.unique(log_data['sc-status'].unique().astype(float).astype(int))
    dest_compared_responses= existing_responses['response_id']

    new_responses_detail, new_responses_counter, escaped_responses_counter= filterNewDimKeys(unique_response_statuses,dest_compared_responses, 'response_id')

    print('old responses: '+ str(escaped_responses_counter) +', new responses: '+ str(new_responses_counter))
    new_responses_detail.to_sql('dim_response', engine, if_exists='append',schema= schema, method='multi', index= False)
    print('Done updating dim_response')


def _updateFiletypeDimension(ti):
    '''Filetype Dimension'''
    engine,db, schema= serverParams()

    try:
        existing_filetypes= pd.read_sql_table('dim_filetype',con= engine, schema= schema)
    except:
        with engine.connect() as con:
            con.execute(createTable('filetype'))
        existing_filetypes= pd.read_sql_table('dim_filetype',con= engine, schema= schema)

    log_data= pd.read_sql_table('staging_IIS_log_data',con= engine, schema= schema)
    all_source_file_extensions= rgetFileExtensions(log_data['cs-uri-stem'])
    source_unique_filetypes= all_source_file_extensions['file_type'].unique()
    dest_compared_uri= existing_filetypes['file_type']

    new_filetypes_detail, new_filetypes_counter, escaped_filetypes_counter= filterNewDimKeys(source_unique_filetypes,dest_compared_uri, 'file_type')

    print('old filetypes: '+ str(escaped_filetypes_counter) +', new filetypes: '+ str(new_filetypes_counter))
    new_filetypes_detail.to_sql('dim_filetype', engine, if_exists='append', schema= schema, method='multi', index= False)
    print('Done updating dim_filetype')


def _updateUriDimension(ti):
    '''URI Dimension'''
    engine,db, schema= serverParams()

    try:
        existing_uris= pd.read_sql_table('dim_uri',con= engine, schema= schema)
    except:
        with engine.connect() as con:
            con.execute(createTable('uri'))
        existing_uris= pd.read_sql_table('dim_uri',con= engine, schema= schema)

    log_data= pd.read_sql_table('staging_IIS_log_data',con= engine, schema= schema)
    source_unique_uri= log_data['cs-uri-stem'].unique()
    dest_compared_uri= existing_uris['uri']
    new_uris_detail, new_uris_counter, escaped_uris_counter=filterNewDimKeys(source_unique_uri,dest_compared_uri, 'uri')

    print('old filetypes: '+ str(escaped_uris_counter) +', new filetypes: '+ str(new_uris_counter))
    new_uris_detail.to_sql('dim_uri', engine, if_exists='append', schema= schema, method='multi', index= False)
    print('Done updating dim_uri')

def _updateRefererDimension(ti):
    '''Referer Dimension'''
    engine,db, schema= serverParams()

    try:
        existing_referer= pd.read_sql_table('dim_referer',con= engine, schema= schema)
    except:
        with engine.connect() as con:
            con.execute(createTable('referer'))
        existing_referer= pd.read_sql_table('dim_referer',con= engine, schema= schema)

    log_data= pd.read_sql_table('staging_IIS_log_data',con= engine, schema= schema)
    all_source_referers= rgetReferers(log_data['cs(Referer)'])
    all_source_referers['referer'] = all_source_referers['referer'].fillna('nil')
    source_unique_referers= all_source_referers['referer'].unique()
    dest_compared_referers= existing_referer['referer']

    new_referer_detail, new_referer_counter, escaped_referer_counter=filterNewDimKeys(source_unique_referers,dest_compared_referers, 'referer')

    print('old referer: '+ str(escaped_referer_counter) +', new referer: '+ str(new_referer_counter))
    new_referer_detail.to_sql('dim_referer', engine, if_exists='append', schema= schema, method='multi', index= False)
    print('Done updating dim_referer')


def _updateOSDimesnion(ti):
    '''OS Dimension'''
    engine,db, schema= serverParams()

    try:
        existing_os= pd.read_sql_table('dim_os',con= engine, schema= schema)
    except:
        with engine.connect() as con:
            con.execute(createTable('os'))
        existing_os= pd.read_sql_table('dim_os',con= engine, schema= schema)

    log_data= pd.read_sql_table('staging_IIS_log_data',con= engine, schema= schema)
    source_unique_os= getUniqueOS(log_data['cs(User-Agent)'])['os']
    dest_compared_os= existing_os['os']

    new_os_detail, new_os_counter, escaped_os_counter=filterNewDimKeys(source_unique_os,dest_compared_os, 'os')

    print('old os: '+ str(escaped_os_counter) +', new os: '+ str(new_os_counter))
    new_os_detail.to_sql('dim_os', engine, if_exists='append', schema= schema, method='multi', index= False)
    print('Done updating dim_os')


def _updateBrowserDimension(ti):
    '''Browser Dimension'''
    engine,db, schema= serverParams()

    try:
        existing_browser= pd.read_sql_table('dim_browser',con= engine, schema= schema)
    except:
        with engine.connect() as con:
            con.execute(createTable('browser'))
        existing_browser= pd.read_sql_table('dim_browser',con= engine, schema= schema)

    log_data= pd.read_sql_table('staging_IIS_log_data',con= engine, schema= schema)
    source_unique_browser= getUniqueBrowsers(log_data['cs(User-Agent)'])['browser']
    dest_compared_browser= existing_browser['browser']

    new_browser_detail, new_browser_counter, escaped_browser_counter=filterNewDimKeys(source_unique_browser,dest_compared_browser, 'browser')

    print('old browser: '+ str(escaped_browser_counter) +', new browser: '+ str(new_browser_counter))
    new_browser_detail.to_sql('dim_browser', engine, if_exists='append', schema= schema, method='multi', index= False)
    print('Done updating dim_browser')




def _updateFactTable(ti):
    engine,db, schema= serverParams()

    log_data= pd.read_sql_table('staging_IIS_log_data',con= engine, schema= schema)
    existing_ips= pd.read_sql_table('dim_users_new',con= engine, schema= schema)
    existing_hosts= pd.read_sql_table('dim_hosts',con= engine, schema= schema)
    existing_referer= pd.read_sql_table('dim_referer',con= engine, schema= schema)
    existing_uris= pd.read_sql_table('dim_uri',con= engine, schema= schema)
    existing_browser= pd.read_sql_table('dim_browser',con= engine, schema= schema)

    #user fact mapping
    df_fact= log_data.merge(existing_ips, left_on='c-ip', right_on='ip', how='left')
    df_fact= df_fact.loc[:, ~df_fact.columns.isin(['c-ip', 'ip', 'city', 'region', 'country','cs-method','s-port', 'cs-username', 'cs(Cookie)', 'sc-substatus', 'sc-win32-status', 'cs-uri-query'])]
    print('user merged', len(df_fact)) 

    # host fact mapping
    df_fact= df_fact.merge(existing_hosts, left_on='s-ip', right_on='ip', how='left')
    df_fact= df_fact.loc[:, ~df_fact.columns.isin(['s-ip', 'ip', 'city', 'region', 'country'])]
    print('host merged', len(df_fact)) 
    df_fact

    # response
    df_fact = df_fact.rename(columns={'sc-status': 'response_id'})
    print('response merged', len(df_fact)) 

    # uri
    df_fact = df_fact.rename(columns={'cs-uri-stem': 'uri_file'})
    df_fact_for_time= df_fact
    print('uri cleaned', len(df_fact)) 

    #time
    new_time_detail= approxTimeMinute(df_fact_for_time['time'])
    df_fact= df_fact.merge(new_time_detail,left_on='time', right_on='time_c', how='left')
    df_fact= df_fact.loc[:, ~df_fact.columns.isin(['time', 'time_c'])]
    print('time merged', len(df_fact)) 

    #filetype
    df_fact['file_extension_id']= rgetFileExtensions(log_data['cs-uri-stem'])
    print('filetype merged', len(df_fact)) 

    #uri
    df_fact= df_fact.merge(existing_uris,left_on='uri_file', right_on='uri', how='left')
    df_fact= df_fact.loc[:, ~df_fact.columns.isin(['uri_file', 'uri'])]
    print('uri merged', len(df_fact)) 

    # referer
    all_source_referers= rgetReferers(log_data['cs(Referer)'])
    all_source_referers['referer'] = all_source_referers['referer'].fillna('nil')
    df_fact['referer_link']= all_source_referers
    df_fact= df_fact.merge(existing_referer,left_on='referer_link', right_on='referer', how='left')
    df_fact= df_fact.loc[:, ~df_fact.columns.isin(['referer_link', 'referer', 'cs(Referer)'])]
    print('referer merged', len(df_fact)) 

    #os
    df_fact['os_name']= rgetOS(log_data['cs(User-Agent)'])
    print('os merged', len(df_fact)) 

    #browser
    df_fact['browser_name']= rgetBrowsers(log_data['cs(User-Agent)'])
    df_fact= df_fact.merge(existing_browser,left_on='browser_name', right_on='browser', how='left')
    df_fact= df_fact.loc[:, ~df_fact.columns.isin(['browser_name', 'browser', 'cs(User-Agent)'])]
    print('browser merged', len(df_fact)) 

    #write fact to table
    df_fact = df_fact[['date', 'time_id', 'host_ip_id', 'response_id', 'user_ip_id', 'os_name', 'referer_id', 'file_extension_id', 'uri_id', 'browser_id', 'time-taken', 'sc-bytes', 'cs-bytes']]
    df_fact.to_sql('fact_logs_freshest', engine, if_exists='append', schema= schema, method='multi', index= False)

    print("'fact_logs_freshest' updated")


with DAG("log_data", start_date=datetime(2023, 3, 30), schedule='@daily', catchup=False) as dag:

    readLogToStaging= PythonOperator(
        task_id='readLogToStaging',
        python_callable=_readLogToStaging
    )
 
    updateUserDimension = PythonOperator(
        task_id='updateUserDimension',
        python_callable=_updateUserDimension
    )

    updateHostDimension = PythonOperator(
        task_id='updateHostDimension',
        python_callable=_updateHostDimension
    )

    updateResponseDimension = PythonOperator(
        task_id='updateResponseDimension',
        python_callable=_updateResponseDimension
    )

    updateRefererDimension = PythonOperator(
        task_id='updateRefererDimension',
        python_callable=_updateRefererDimension
    )

    updateFiletypeDimension = PythonOperator(
        task_id='updateFiletypeDimension',
        python_callable=_updateFiletypeDimension
    )
    updateUriDimension = PythonOperator(
        task_id='updateUriDimension',
        python_callable=_updateUriDimension
    )

    updateOSDimesnion = PythonOperator(
        task_id='updateOSDimesnion',
        python_callable=_updateOSDimesnion
    )

    updateBrowserDimension = PythonOperator(
        task_id='updateBrowserDimension',
        python_callable=_updateBrowserDimension
    )

    updateFactTable = PythonOperator(
        task_id='updateFactTable',
        python_callable=_updateFactTable
    )

readLogToStaging >> [updateUserDimension,updateHostDimension, updateResponseDimension, updateFiletypeDimension, updateUriDimension, updateRefererDimension,updateOSDimesnion, updateBrowserDimension] >> updateFactTable

