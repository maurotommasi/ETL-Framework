import pandas as pd

class Loader:

    def __init__(self, ID, save_config):
        self.ID = ID
        self.data = None
        self.control = None
        self.save_config = save_config

    def set_data(self, data):
        self.data = data

    def set_safe_config(self, save_config):
        self.save_config = save_config
    
    def set_control(self, control):
        self.control = control
    
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

class LoaderConfig:

    def __init__(self):
        None

    def json_file_loader(self, path):
        return {
            "loader_destination_type": "file",
            "loader_destination_format": "json",
            "loader_destination_path": path
        }