import pandas as pd

class Loader:

    def __init__(self, ID, save_config):
        self.ID = ID
        self.data = None
        self.control = None
        self.save_config = save_config

    def set_data(self, df):
        self.data = df

    def set_safe_config(self, save_config):
        self.save_config = save_config
    
    def set_control(self, control):
        self.control = control
    
    def save_to_file(self, file_path = "export", export_format='csv'):
        if export_format == 'csv':
            file_path = f"{file_path}.csv"
            pd.DataFrame(self.data).to_csv(file_path, index=False)
            print(f'DataFrame exported as CSV: {file_path}')
        elif export_format == 'excel':
            file_path = f"{file_path}.xlsx"
            pd.DataFrame(self.data).to_excel(file_path, index=False)
            print(f'DataFrame exported as Excel: {file_path}')
        elif export_format == 'json':
            file_path = f"{file_path}.json"
            pd.DataFrame(self.data).to_json(file_path, orient='records')
            print(f'DataFrame exported as JSON: {file_path}')
        else:
            print('Invalid export format specified. Supported formats: csv, excel, json, sqlite')

 