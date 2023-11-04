import os
import pandas as pd

class Loader:

    def __init__(self, ID, save_config, custom_loader = None):
        self.ID = ID
        self.data = None
        self.control = None
        self.save_config = save_config
        self.encoder = None
        self.process = None
        self.custom_loader = custom_loader

    def set_data(self, data):
        if len(data) > 0:
            self.data = data.rename(columns=self.encoder.get_encoder()['mapping'])
        else:
            self.data = data

    def set_safe_config(self, save_config):
        self.save_config = save_config
    
    def set_encoder(self, encoder):
        self.encoder = encoder

    def set_control(self, control):
        self.control = control
    
    def set_process(self, process):
        self.process = process

    def run_custom_loader(self):
        self.custom_loader(self.data)

class LoaderConfig:

    def __init__(self):
        None
    
    def custom_loader(self):
        return {
            "loader_destination_type": "custom"
        }