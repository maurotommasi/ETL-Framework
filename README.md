# Python ETL Multithread Custom Process

The provided Python code defines an ETL (Extract, Transform, Load) process in a multi-thread class based infrastructure.

# ETL Class Structure:

The Framework is able to manage different flows of acquisition, trasformation and loading from a DataSource to a final place.
The framework schema follow the figure:

<img src="./FrameworkSchema.png">

We can see, from the image how the framework works:
1) We have 3 flows (or streams) that start with some data from DataSource, DataSource 2 and DataSource 3.
2) Every DataSource has an Encoder that defines the fields in input (from where? which fields?) and the field in output (to where? which fields?) of the flow
3) The data will be downloaded from the DataSource using the method defined inside the DataSource
4) The data can be transformed as needed
5) The Flow Control System will define when and how often the flow has to be run. The FCS generates a new thread on the python process for each flow, saving CPUs on the waiting time
6) All result data will be saved/uploaded as needed

Ingestions, Trasformations and Loaders are fully customizable. The framework is on development, it provides some predefined, but limited, functionalities but it is possible to unlock all his potential with custom python functions.

## Libraries to import

The main library to be able to use the Framework is:
```python
from ETL.Pipeline import Pipeline
```

To be able to run properly the ETL pipeline we need to implement this list of libraries:

```python
from ETL.Pipeline import Pipeline, FlowConfig
from ETL.modules.DataSource import DataSourceConfig
from ETL.modules.Encoder import EncoderConfig
from ETL.modules.Loader import LoaderConfig
from ETL.modules.Utils import Utils
from datetime import datetime
```

Those libraries will help us to manage easily some configuration in few lines of code.

## DataSource:
Datasource defines which kind of source we are facing on. Right now we identify:
- URL DataSource
- MySQL DataSource
- Custom DataSource

Each of them has his own configuration as follow:

### URL DataSource

This kind of DataSource will use BeautifulSoap4 to be able to scraping content from the website.
Based on the tags defined inside the encoder it will extract all date into a dataframe.
It's suggested for quick data acquisition. More advanced scraping tecniques can be done thanks to the Custom DataSource.
```python
# Datasource Config
url = "https://www.google.com"
ds_config = DataSourceConfig().scraping_source_config(url)
```
This will create a dictionary as follow:
```python
def scraping_source_config(self, url):
    return {
        "source_type": "url",
        "source_response": "plain_text",
        "url": url
    }
```
<b>Note:</b> source_type key is mandatory for each configuration.

### MySQL DataSource

```python
# Datasource Config
ds_config = DataSourceConfig().mysql_source_config("127.0.0.1", "root", "", "mt-engineering", "Select * from wp_postmeta")
```
This will create a dictionary as follow:
```python
def query_source_config(self, host, username, password, database, query):
    return {
        "source_type": "query_mysql",
        "username": username,
        "password": password,
        "database": database,
        "host": host,
        "query": query
    }
```

### Custom DataSource

```python
# Datasource Config
ds_config = DataSourceConfig().custom_source_config()
```
This will create a dictionary as follow:
```python
def custom_source_config(self):
    return {
        "source_type": "custom"
    }
```

### Set DataSource in the Pipeline flow:
```python
pipeline.create_datasource(ID_Datasource = 'DS1', datasource_config = ds_config)
```

## Encoder:

The encoder tell us the type of encoder and the mapping between the source fields and the loader fields. For example:
```python
{
    "encoder_type": "html",
    "mapping": {
        "h1": "h1_m",
        "h2": "h2_m",
        "h3": "h3_m",
        "h4": "h4_m",
        "h5": "h5_m",
        "h6": "h6_m",
        "p": "p_m",
        "a": "a_m",
        "img": "img_m"
    }
}
```
This dictionary can be achieved using the html_encoder_config, mysql_encoder_config, custom_encoder_config. They will require a list for the source attributes and a list for the loader attributes in a 1-to-1 relationship.
The example above is equals to:
```python
tags_from_source = ["h1", "h2", "h3", "h4", "h5", "h6", "p", "a", "img"]
tags_to_loader = ["h1_m", "h2_m", "h3_m", "h4_m", "h5_m", "h6_m", "p_m", "a_m", "img_m"]        

en_config = EncoderConfig().html_encoder_config(tags_from_source, tags_to_loader)
```

### Set Encoder in the Pipeline flow:
```python
pipeline.create_encoder(ID_Encoder = 'EN1', encoder_config = en_config)
```

## Ingestion:

The process of ingestion will allow us to perform the "Fetching" operation of the ETL pipeline.

Right now we defined two standard ingestions and one fully customizable.

### Standard Ingestion

After having created a DataSource with the url from where we will get the data and the encoder that will define which kind of operation will be run in the ingestion process, the standard scraping function will not require any additional parameter in the creation of the ingestion istance.

The ingestion for standard functions will take:
1) All the data from all the tags if the DataSource source_type is 'url'
2) All the data from a query MySQL if the the DataSource source_type is 'query_mysql'
```python
pipeline.create_ingestion(ID_Ingestion = 'DS1')
```

### Custom Ingestion

We can create a custom ingestion function and set it in the ingestion istance
```python
# Ingestion Function

def query_wp_post(feedback):
    df = pd.DataFrame()
    latest_date = ""
    # Ensure feedback variable is initialized
    if "latest_date" in feedback:
        latest_date = feedback['latest_date']
    try:
        host = "127.0.0.1"
        user = "root"
        password = ""
        database = "mt-engineering"
        connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
        if connection.is_connected():
            query = f"Select post_title, post_status, post_date from wp_posts where post_date > '{latest_date}'"
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

# Ingestion Istance

pipeline.create_ingestion(ID_Ingestion = 'IN1', custom_function = query_wp_post)
```

The ingestion process has to return:
1) A Dataframe with the that has to be transformed and loaded
2) A Dictionary with the feedback data from the ingestion execution

In the example the ingestion will take only the data from the table if there will be new data from previous ingestion.

All the libraries used for ingestion has to be imported in the ingestion file class in ./ETL/modules/Ingestion.py.

## Transformation:

The transformation (or process) allow us to transform the content of a DataFrame with a list of funtions. It will take a dataframe such an input (from the ingestion) and it will release a dataframe such output.

```python
# Transformation Function

def upper_meta_value(df):
    df['post_status'] = df['post_status'].str.upper()
    return df

pipeline.create_process(ID_process = 'TR1', [upper_meta_value, lambda x: x * 2])
```

After the ingestion the transformation process will elaborate all the functions, in order, contained in the function list

Default transformation function are already defined and new ones will be created.

For example:

```python
# Transformation.py

def default_scraping_to_pivot_table(self, df):
    print("Running pivot_table transformation.")
    df = df.drop_duplicates(subset=['original_tag', 'text', 'url', 'date_time', 'ingestion_ID'])
    pivoted_df = df.pivot(index=['text', 'url', 'date_time', 'ingestion_ID'], columns='mapping_tag', values='text').reset_index()
    pivoted_df.columns.name = None
    pivoted_df.fillna('', inplace=True)
    return pivoted_df
```

## Control
The control istance will allow us to set frequency and timing of the flow
```python
# Create Started date/time

date_string = "2023-11-03 00:00:00"
date_format = "%Y-%m-%d %H:%M:%S"
start_datetime = datetime.strptime(date_string, date_format)

pipeline.create_control(ID_Control = 'CL1', pause = 3600, pause_if_error = 3600 * 24, start_from = start_datetime)
```

In this case the ingestion will be excuted the 3rd November 2023 at midnight and it's scheduled every hour to be run.
If the start_from is based in the past, it will be considered the date-time scheduled after the current date-time. This mean if we are at 3rd November 2023 at 00:15, we have to wait next execution in 45 minutes. Based on the settings,, we suppose to have a run at 1.00 AM. 
A pause of 0 seconds is not allowed.
We defined also a pause_if_error that will stop the ingestion for a specific amount of seconds (1 day in the example).

## Loader

A loader is the act of saving/upload the data. It will receive the data from the ingestion/process such a dataframe and it will load the data somewhere.

For example:

```python
# Loader Config / Function

ld_config = LoaderConfig().custom_loader()

def append_to_file_csv(df):
    print(df)
    cols_dest = ['meta_id', 'POST_STATUS_UPPER']
    path = "./export/export_test.csv"
    print(path)
    if os.path.exists(path):
        result = pd.concat([pd.read_csv(path), df])
        result.to_csv(path, index=True)
    else:
        df.to_csv(path, index=True)

pipeline.create_loader(ID_Loader = 'LD1', loader_config = ld_config, custom_function = append_to_file_csv)
```

In this case the loader will append the content in a specific path in a csv file.

A default loader is already set up to download and save a dataframe. In loader.py module we can see:
```python
 def save_to_file(df, file_path = "export", export_format='csv'):
        if export_format == 'csv':
            file_path = f"{file_path}"
            df.to_csv(file_path, index=False)
            print(f'DataFrame exported as CSV: {file_path}')
        elif export_format == 'excel':
            file_path = f"{file_path}"
            df.to_excel(file_path, index=False)
            print(f'DataFrame exported as Excel: {file_path}')
        elif export_format == 'json':
            file_path = f"{file_path}"
            df.to_json(file_path, orient='records')
            print(f'DataFrame exported as JSON: {file_path}')
        else:
            print('Invalid export format specified. Supported formats: csv, excel, json, sqlite')
```

In the future standard loaders will be part of the framework upgrade.
## Flow
A flow identify the "what" has to be run to complete the ingestion till the data loading.
```python
# Flow Config

flow_config = FlowConfig()

flow1 = flow_config.create_flow(
    ID_datasource='DS1',
    ID_encoder='EN1',
    ID_ingestion='IN1',
    ID_control='CL1',
    ID_process='TR1',
    ID_loader='LD1')

flow2 = flow_config.create_flow(
    ID_datasource='DS2',
    ID_encoder='EN1',
    ID_ingestion='IN1',
    ID_control='CL1',
    ID_loader='LD1')

flow_config.add_flow(flow1)
flow_config.add_flow(flow2)

pipeline.set_flow(flow_config.get_flow_config())
```

In this case we defined two flows, one with a DataSouce DS1 and the other one with a DS2. The encoder are for both the same, this means we are fetching he same fields in input and we will have th same fields in output. The process of ingestion is always the same, as it's the same the loader function.
However the second flow doesn't have the process defined, this mean the ingestion will be not transformed at all.

## Start the flow

After having setup the pipeline flow we can start ETL process using:
```python
pipeline.start()
```
<!--
# Real Case Use

## Object interchangability

## Multi Pipelines using Shell Scripting

## Logging and Issues Control

## Automation Workflow

## Database Backup 

## File Synchronization
-->