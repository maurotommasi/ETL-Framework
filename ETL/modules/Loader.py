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

    def save_to_file(self, file_path = "export", export_format='csv'):
        if export_format == 'csv':
            file_path = f"{file_path}"
            self.data.to_csv(file_path, index=False)
            print(f'DataFrame exported as CSV: {file_path}')
        elif export_format == 'excel':
            file_path = f"{file_path}"
            self.data.to_excel(file_path, index=False)
            print(f'DataFrame exported as Excel: {file_path}')
        elif export_format == 'json':
            file_path = f"{file_path}"
            self.data.to_json(file_path, orient='records')
            print(f'DataFrame exported as JSON: {file_path}')
        else:
            print('Invalid export format specified. Supported formats: csv, excel, json, sqlite')

    def run_custom_loader(self):
        self.custom_loader(self.data)

class LoaderConfig:

    def __init__(self):
        None

    def json_file_loader(self, path):
        return {
            "loader_destination_type": "file",
            "loader_destination_format": "json",
            "loader_destination_path": path
        }
    
    def custom_loader(self):
        return {
            "loader_destination_type": "custom"
        }