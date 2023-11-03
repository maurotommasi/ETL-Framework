# Import necessary modules and classes
from modules.DataSource import DataSource
from modules.Ingestion import Ingestion
from modules.Utils import Utils
from modules.Process import Process
from modules.Control import Control
from modules.Encoder import Encoder
from modules.Loader import Loader
import threading
import time
from datetime import datetime, timedelta

# Define the main ETL class
class ETL():

    # Initialize the ETL class with necessary attributes
    def __init__(self, name, log = True):
        # Initialize class attributes
        self.name = name        
        self.data_sources = {}
        self.ingestions =  {}
        self.processes = {}
        self.encoders = {}
        self.loaders = {}
        self.controls = {}
        self.flow = {}
        self.threads = []
        self.log = log

    # Methods for creating ETL objects

    def create_datasource(self, ID_datasource, data_source_config):
        self.data_sources[ID_datasource] = DataSource(ID_datasource, data_source_config)
    
    def create_encoder(self, ID_encoder, encoder_map):
        self.encoders[ID_encoder] = Encoder(ID_encoder, encoder_map)

    def create_ingestion(self, ID_ingestion, custom_function = None):
        self.ingestions[ID_ingestion] = Ingestion(ID_ingestion, custom_function)

    def create_process(self, ID_process, function_list):
        self.processes[ID_process] = Process(ID_process, function_list)

    def create_loader(self, ID_loader, save_config):
        self.loaders[ID_loader] = Loader(ID_loader, save_config)

    def create_control(self, ID_control, pause = 3600, pause_if_error = 3600, start_datetime = datetime.now()):
        self.controls[ID_control] = Control(ID_control, pause, pause_if_error, start_datetime)
  
    # Methods for linking ETL objects

    def __link_encoder_to_datasource(self, ID_encoder, ID_datasource):
        self.data_sources[ID_datasource].set_encoder(self.encoders[ID_encoder])

    def __link_datasource_to_ingestion(self, ID_datasource, ID_ingestion):
        self.ingestions[ID_ingestion].set_data_source(self.data_sources[ID_datasource])

    def __link_control_to_ingestion(self, ID_control, ID_ingestion):
        self.ingestions[ID_ingestion].set_control(self.controls[ID_control])

    def __link_ingestion_to_loader(self, ID_ingestion, ID_loader):
        self.loaders[ID_loader].set_data(self.ingestions[ID_ingestion].data)

    def __link_encoder_to_loader(self, ID_encoder, ID_Loader):
        self.loaders[ID_Loader].set_encoder(self.encoders[ID_encoder])

    def __link_control_to_loader(self, ID_control, ID_loader):
        self.loaders[ID_loader].set_control(self.controls[ID_control])

    # Method for setting ETL flow configuration from a URL

    def set_flow_from_url(self, flow_mapping_url):
        self.flow = Utils().read_json(flow_mapping_url)
    
    # Method for setting ETL flow configuration

    def set_flow(self, flow):
        self.flow = flow

    # Method to start the ETL process

    def start(self):

        # Log ETL pipeline flow information if logging is enabled
        self.__log(f"Linking Pipeline flows for {self.name}") if self.log else None

        flow_index = 1

        # Iterate through the flow configuration and set up connections and threads
        for flow in self.flow['flow_list']:

            # Make flow connections
            self.__link_encoder_to_datasource(flow["ID_encoder"], flow["ID_datasource"])
            self.__link_datasource_to_ingestion(flow["ID_datasource"], flow["ID_ingestion"])
            self.__link_control_to_ingestion(flow["ID_control"], flow["ID_ingestion"])
            if "ID_process" in flow:
                process = self.processes[flow["ID_process"]]
            else:
                process = None
                self.__link_ingestion_to_loader(flow["ID_ingestion"], flow["ID_loader"])
            self.__link_encoder_to_loader(flow["ID_encoder"], flow["ID_loader"])
            self.__link_control_to_loader(flow["ID_control"], flow["ID_loader"])

            # Create thread to run the ETL pipeline
            self.threads.append(threading.Thread(target=self.__batch_run, args=(self.ingestions[flow["ID_ingestion"]], process, self.loaders[flow["ID_loader"]])))

            flow_index += 1

        # Iterate through the flow configuration and set up connections and threads
        for thread in self.threads:
            thread.start()

        for thread in self.threads:
            thread.join()

    # Method for batch processing data ingestion, transformation, and loading
    def __batch_run(self, ingestion, process, loader):    
        
        dt_start = loader.control.start_from

        # Skip past ingestions

        while datetime.now() > dt_start:
            dt_start = dt_start + timedelta(seconds=loader.control.pause) 
        
        dt_base = dt_start
        dt_index = 1

        while True:

            dt_ingestion = datetime.now()

            if dt_ingestion > dt_start:

                # Ingestion
                
                dt_ingestion_start = datetime.now()
                self.__log(f'Running Ingestion {ingestion.ID}...') if self.log else None
                ingestion.from_html() if ingestion.data_source.data_source['source_type'] == 'url' else None # Scraping
                ingestion.from_mysql() if ingestion.data_source.data_source['source_type'] == 'mysql' else None # MySQL
                data = ingestion.data
                self.__log(f'Ingestion {ingestion.ID} successfully completed!') if self.log else None

                # Process

                if process is not None:
                    if len(process.function_list) > 0:
                        self.__log(f'Running Transformation {process.ID} for Ingestion {ingestion.ID}...') if self.log else None
                        data = process.execute(data) 
                        self.__log(f'Trasformation {process.ID} for ingestion {ingestion.ID} Done!') if self.log else None

                # Loader  

                self.__log(f'Loading data from Ingestion {ingestion.ID}...') if self.log else None
                loader.set_data(data)
                loader.save_to_file(loader.save_config["loader_destination_path"], loader.save_config["loader_destination_format"]) if loader.save_config["loader_destination_type"] == "file" else None
                self.__log(f'Loaded data from Ingestion {ingestion.ID}!') if self.log else None

                # Feedback Control

                dt_start = dt_base + dt_index * timedelta(seconds=loader.control.pause)
                dt_index += 1
                self.__log(f'Next execution for {ingestion.ID} will be at {dt_start}') if self.log else None

                # CPU Saver

                time.sleep(loader.control.pause - (datetime.now() - dt_ingestion_start).total_seconds()) if datetime.now() > dt_ingestion_start else None

            else:
                self.__log(f"Wait for first execution at {dt_start} for {ingestion.ID}")
                time.sleep((dt_start - dt_ingestion).total_seconds()) # Free CPU

    # Method for logging messages and appending them to a log file

    def __log(self, content):
        print(content)
        Utils().append_to_file('log.txt', f'{datetime.now()}: {content}')

# Class for managing flow configurations

class FlowConfig:

    # Initialize the FlowConfig class with an empty list for flow configurations
    def __init__(self):
        self.flow_list = []

    # Method for adding a flow configuration to the list
    def add_flow(self, flow):
        self.flow_list.append(flow)

    # Method for creating a flow configuration 
    def create_flow(self, ID_datasource, ID_encoder, ID_ingestion, ID_control, ID_loader, ID_process = ""):
        if len(ID_process) > 0:
            return {
                "ID_encoder": ID_encoder,
                "ID_datasource": ID_datasource,
                "ID_ingestion": ID_ingestion,
                "ID_process": ID_process,
                "ID_loader": ID_loader,
                "ID_control": ID_control
            }
        else:
            return {
                "ID_encoder": ID_encoder,
                "ID_datasource": ID_datasource,
                "ID_ingestion": ID_ingestion,
                "ID_loader": ID_loader,
                "ID_control": ID_control
            }
        
    # Method for retrieving the complete flow configuration    
    def get_flow_config(self):
        return {
            "flow_list": self.flow_list
        }
