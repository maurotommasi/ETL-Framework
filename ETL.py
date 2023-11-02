from modules.DataSource import DataSource
from modules.Ingestion import Ingestion
from modules.Utils import Utils
from modules.Process import Process
from modules.Control import Control
from modules.Encoder import Encoder
from modules.Loader import Loader

import threading
import time
from datetime import datetime

class ETL():

    def __init__(self, name, log = True):
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

    # Creation of ETL Objects

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

    def create_control(self, ID_control, pause = 3600, pause_if_error = 3600):
        self.controls[ID_control] = Control(ID_control, pause, pause_if_error)
  
    # Link the ETL Objects

    def __link_encoder_to_datasource(self, ID_encoder, ID_datasource):
        self.data_sources[ID_datasource].set_encoder(self.encoders[ID_encoder])

    def __link_datasource_to_ingestion(self, ID_datasource, ID_ingestion):
        self.ingestions[ID_ingestion].set_data_source(self.data_sources[ID_datasource])

    def __link_control_to_ingestion(self, ID_control, ID_ingestion):
        self.ingestions[ID_ingestion].set_control(self.controls[ID_control])

    def __link_ingestion_to_loader(self, ID_ingestion, ID_loader):
        self.loaders[ID_loader].set_data(self.ingestions[ID_ingestion].data)

    def __link_control_to_loader(self, ID_control, ID_loader):
        self.loaders[ID_loader].set_control(self.controls[ID_control])

    # Flow Control

    def set_flow_from_url(self, flow_mapping_url):
        self.flow = Utils().read_json(flow_mapping_url)

        """
        Flow Json File Sample:

        {
            "flow_list": [
                {
                    "ID_encoder": ID_encoder,
                    "ID_datasource": ID_datasource,
                    "ID_ingestion": ID_ingestion,
                    "ID_process": ID_process, # Can be Null
                    "ID_loader": ID_loader,
                    "ID_control": ID_control
                },
                {
                    "ID_encoder": ID_encoder,
                    "ID_datasource": ID_datasource,
                    "ID_ingestion": ID_ingestion,
                    "ID_process": ID_process, # Can be Null
                    "ID_loader": ID_loader,
                    "ID_control": ID_control
                },
            ]
        }

        Every flow will be a thread
        """
    
    def set_flow(self, flow):
        self.flow = flow

    def start(self):

        self.__log(f"Linking Pipeline flows for {self.name}") if self.log else None

        flow_index = 1

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
            self.__link_control_to_loader(flow["ID_control"], flow["ID_loader"])

            # Create thread to run the ETL pipeline
            self.threads.append(threading.Thread(target=self.__batch_run, args=(self.ingestions[flow["ID_ingestion"]], process, self.loaders[flow["ID_loader"]])))

            flow_index += 1

        for thread in self.threads:
            thread.start()

        for thread in self.threads:
            thread.join()

    def __batch_run(self, ingestion, process, loader):    

        while True:
            # Ingestion
            self.__log(f'Running Ingestion {ingestion.ID}...') if self.log else None

            ingestion.from_html() if ingestion.data_source.data_source['source_type'] == 'url' else None # Scraping

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

            self.__log(f'Next execution for {ingestion.ID} will be in {loader.control.pause} seconds') if self.log else None
            time.sleep(loader.control.pause)

    def __log(self, content):
        print(content)
        Utils().append_to_file('log.txt', f'{datetime.now()}: {content}')

class FlowConfig:

    def __init__(self):
        self.flow_list = []

    def add_flow(self, flow):
        self.flow_list.append(flow)
        
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
    
    def get_flow_config(self):
        return {
            "flow_list": self.flow_list
        }
