from ETL.Pipeline import Pipeline, FlowConfig
from ETL.modules.DataSource import DataSourceConfig
from ETL.modules.Encoder import EncoderConfig
from ETL.modules.Loader import LoaderConfig
from ETL.modules.Utils import Utils
from datetime import datetime
import mysql.connector
import pandas as pd
import os
# Datasource Config

ds_config = DataSourceConfig().custom_source_config()

# Ingestion Function

def query_wp_post(feedback):
    df = pd.DataFrame()
    latest_date = ""
    if "latest_date" in feedback:
        latest_date = feedback['latest_date']
    try:
        host = "127.0.0.1"
        user = "root"
        password = ""
        database = "mt-engineering"
        connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
        if connection.is_connected():
            query = f"Select post_title, post_status, post_date from wp_posts"# where post_date > '{latest_date}'"
            cursor = connection.cursor()
            cursor.execute(query)
            column_names = [description[0] for description in cursor.description]
            results = cursor.fetchall()
            if len(results) > 0:
                df = pd.DataFrame(results, columns=column_names)
                df["date_time_execution"] = datetime.now()
                feedback['latest_date'] = max(df['post_date'])
            cursor.close()
            connection.close()
        else:
            print(f"Not Connected to host: {host} - database: {database}")
    except mysql.connector.Error as err:
        print("Error: ", err)
    return df, feedback

def query_wp_postmeta(feedback):
    df = pd.DataFrame()
    try:
        host = "127.0.0.1"
        user = "root"
        password = ""
        database = "mt-engineering"
        connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
        if connection.is_connected():
            query = f"Select meta_id from wp_postmeta"
            cursor = connection.cursor()
            cursor.execute(query)
            column_names = [description[0] for description in cursor.description]
            results = cursor.fetchall()
            if len(results) > 0:
                df = pd.DataFrame(results, columns=column_names)
                df["date_time_execution"] = datetime.now()
            cursor.close()
            connection.close()
        else:
            print(f"Not Connected to host: {host} - database: {database}")
    except mysql.connector.Error as err:
        print("Error: ", err)
    return df, feedback

# Encoder Config

cols_source = ['post_title', 'post_status']
cols_dest = ['meta_id', 'POST_STATUS_UPPER']
en_config = EncoderConfig().custom_encoder_config(cols_source, cols_dest)

cols_source = ['meta_id']
cols_dest = ['META_ID']
en_config2 = EncoderConfig().custom_encoder_config(cols_source, cols_dest)

# Transformation Function

def upper_meta_value(df):
    df['post_status'] = df['post_status'].str.upper()
    return df

# Loader Config / Function

ld_config = LoaderConfig().custom_loader()

def append_to_file_csv_1(df):
    path = "./export/excel_export_test.csv"
    if os.path.exists(path):
        result = pd.concat([pd.read_csv(path), df])
        result.to_csv(path, index=True)
    else:
        df.to_csv(path, index=True)

def append_to_file_csv_2(df):
    path = "./export/excel_export_test_2.csv"
    if os.path.exists(path):
        result = pd.concat([pd.read_csv(path), df])
        result.to_csv(path, index=True)
    else:
        df.to_csv(path, index=True)

# Control Config - Create Started date/time

date_string = "2023-11-01 00:00:00"
date_format = "%Y-%m-%d %H:%M:%S"
start_datetime = datetime.strptime(date_string, date_format)

# Pipeline Object Creation

pipeline = Pipeline("ETL Pipeline")

# Create Pipeline Elements

## Flow 1 Instances

pipeline.create_datasource('DS1', ds_config)
pipeline.create_encoder('EN1', en_config)
pipeline.create_ingestion('IN1', query_wp_post)
pipeline.create_control('CT1', 30, 3600, start_datetime)
pipeline.create_process('TR1', [upper_meta_value])
pipeline.create_loader('LD1', ld_config, append_to_file_csv_1)

## Flow 2 New Istances

pipeline.create_ingestion('IN2', query_wp_postmeta)
pipeline.create_encoder('EN2', en_config2)
pipeline.create_control('CT2', 60, 3600, start_datetime)
pipeline.create_loader('LD2', ld_config, append_to_file_csv_2)

# Flow Config

flow_config = FlowConfig()

flow1 = flow_config.create_flow(
    ID_datasource='DS1',
    ID_encoder='EN1',
    ID_ingestion='IN1',
    ID_control='CT1',
    ID_process='TR1',
    ID_loader='LD1')

flow2 = flow_config.create_flow(
    ID_datasource='DS1',
    ID_encoder='EN2',
    ID_ingestion='IN2',
    ID_control='CT2',
    ID_loader='LD2')

flow_config.add_flow(flow1)
flow_config.add_flow(flow2)

# Set Pipeline flow

pipeline.set_flow(flow_config.get_flow_config())

# Pipeline Start 

pipeline.start()