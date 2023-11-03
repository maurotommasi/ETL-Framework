class DataSource:

    def __init__(self, ID_data_source, data_source_config):
        self.ID = ID_data_source
        self.data_source = data_source_config
        self.data_source_args = {}
        self.source_response_type = 'plain_text'
        self.encoder = None

    def set_encoder(self, encoder):
        self.encoder = encoder

    def set_data_source(self, data_source_config):
        if data_source_config['source_type'] == 'url':
            self.set_url(data_source_config['url'])
        elif data_source_config['source_type'] == 'mysql':
            self.set_mysql(data_source_config['host'], 
                           data_source_config['username'], 
                           data_source_config['password'], 
                           data_source_config['database'],
                           data_source_config['query'])
    def get_data_source(self):
        return self.data_source
        
    def get_encoder(self):
        return self.encoder
    
class DataSourceConfig:

    def __init__(self):
        None

    def scraping_source_config(self, url):
        return {
            "source_type": "url",
            "source_response": "plain_text",
            "url": url
        }
    
    def query_source_config(self, host, username, password, database, query):
        return {
            "source_type": "query_mysql",
            "username": username,
            "password": password,
            "database": database,
            "host": host,
            "query": query
        }
    
    def custom_source_config(self):
        return {
            "source_type": "custom"
        }