from modules.DataSource import DataSource
from modules.Ingestion import Ingestion
from modules.Utils import Utils
from modules.Process import Process
from modules.Control import Control
from modules.Encoder import Encoder
from modules.Loader import Loader

import threading
import time

class ETL():

    def __init__(self, name):
        self.name = name        
        self.data_sources = {}
        self.ingestions =  {}
        self.processes = {}
        self.encoders = {}
        self.loaders = {}
        self.controls = {}
        self.flow = {}
        self.threads = []

    # Creation of ETL Objects

    def create_datasource(self, ID_datasource, data_source_config):
        self.data_sources[ID_datasource] = DataSource(ID_datasource, data_source_config)
    
    def create_encoder(self, ID_encoder, encoder_map):
        self.encoders[ID_encoder] = Encoder(ID_encoder, encoder_map)

    def create_ingestion(self, ID_ingestion):
        self.ingestions[ID_ingestion] = Ingestion(ID_ingestion)

    def create_process(self, ID_process):
        self.processes[ID_process] = Process(ID_process)

    def create_loader(self, ID_loader, save_config):
        self.loaders[ID_loader] = Loader(ID_loader, save_config)

    def create_control(self, ID_control, pause = 3600, pause_if_error = 3600):
        self.controls[ID_control] = Control(ID_control, pause, pause_if_error)

    def create_all(self, ID):
        self.data_sources[ID] = DataSource(ID)
        self.encoders[ID] = Encoder(ID)
        self.ingestions[ID] = Ingestion(ID)
        self.processes[ID] = Process(ID)
        self.loaders[ID] = Loader(ID)
        self.loaders[ID] = Control(ID)
  
    # Link the ETL Objects

    def __link_encoder_to_datasource(self, ID_encoder, ID_datasource):
        self.data_sources[ID_datasource].set_encoder(self.encoders[ID_encoder])

    def __link_datasource_to_ingestion(self, ID_datasource, ID_ingestion):
        self.ingestions[ID_ingestion].set_data_source(self.data_sources[ID_datasource])

    def __link_control_to_ingestion(self, ID_control, ID_ingestion):
        self.ingestions[ID_ingestion].set_control(self.controls[ID_control])

    def __link_ingestion_to_process(self, ID_ingestion, ID_process):
        self.processes[ID_process].set_ingestion(self.ingestions[ID_ingestion])

    def __link_ingestion_to_loader(self, ID_ingestion, ID_loader):
        self.loaders[ID_loader].set_data(self.ingestions[ID_ingestion].data)

    def __link_control_to_loader(self, ID_control, ID_loader):
        self.loaders[ID_loader].set_control(self.controls[ID_control])

    def __link_process_to_loader(self, ID_process, ID_loader):
        self.loaders[ID_loader].set_data(self.processes[ID_process])

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

        print("Linking Pipeline flows")

        flow_index = 1

        for flow in self.flow['flow_list']:
            
            print(f'Flow index: {flow_index} saved and ready to be run.')
            # Make flow connections
            
            self.__link_encoder_to_datasource(flow["ID_encoder"], flow["ID_datasource"])
            self.__link_datasource_to_ingestion(flow["ID_datasource"], flow["ID_ingestion"])
            self.__link_control_to_ingestion(flow["ID_control"], flow["ID_ingestion"])
            if "ID_process" not in flow:
                process = self.processes[flow["ID_process"]]
                self.__link_ingestion_to_process(flow["ID_ingestion"], flow["ID_process"]) 
                self.__link_process_to_loader(flow["ID_process"], flow["ID_loader"])
            else:
                process = None
                self.__link_ingestion_to_loader(flow["ID_ingestion"], flow["ID_loader"])
            self.__link_control_to_loader(flow["ID_control"], flow["ID_loader"])

            # Create thread to run the ETL pipeline
            self.threads.append(threading.Thread(target=self.__batch_run, args=(self.ingestions[flow["ID_ingestion"]], process, self.loaders[flow["ID_loader"]])))

        for thread in self.threads:
            thread.start()
            thread.join()

    def __batch_run(self, ingestion, process, loader):    

        while True:
            # Ingestion
            
            print(f'Running Ingestion {ingestion.ID}...')
            
            encoder = ingestion.data_source.get_encoder().get_encoder()
            print(encoder)
            ingestion.extract_text_from_website() if encoder['encoder_type'] == 'html' else None
            ingestion.extract_text_from_website() if encoder['encoder_type'] == 'json' else None
            ingestion.extract_text_from_website() if encoder['encoder_type'] == 'csv' else None
            ingestion.extract_text_from_website() if encoder['encoder_type'] == 'xml' else None
            ingestion.extract_text_from_website() if encoder['encoder_type'] == 'plain_text' else None
            print(ingestion.data)
            print(f'Ingestion {ingestion.ID} successfully completed!')

            # Process

            if process is not None:
                if process.transformation_function is not None:
                    print(f'Running Transformation {process.ID} for Ingestion {ingestion.ID}...')
                    data = process.execute_list(data) 
                    print(f'Trasformation {process.ID} for ingestion {ingestion.ID} Done!')

            # Loader  
            
            print(f'Loading data from Ingestion {ingestion.ID}...')

            loader.save_to_file(loader.save_config["loader_destination_path"], loader.save_config["loader_destination_format"]) if loader.save_config["loader_destination_type"] == "file" else None

            print(f'Loaded data from Ingestion {ingestion.ID}!')

            time.sleep(loader.control.pause)