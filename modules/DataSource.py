import mysql.connector
  
class DataSource:

    def __init__(self, ID_data_source, data_source_config):
        self.ID = ID_data_source
        self.data_source = data_source_config
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
                           data_source_config['database'])

    def set_url(self, url):
        self.data_source = url
        
    def set_mysql(self, host, username, password, database):
        try:
            # Establish a connection to the MySQL database
            connection = mysql.connector.connect(
                host=host,
                user=username,
                password=password,
                database=database
            )

            if connection.is_connected():
                print("Connected to MySQL database")
                self.data_source = connection

        except mysql.connector.Error as err:
            print("Error: ", err)

    def get_data_source(self):
        return self.data_source
        
    def get_encoder(self):
        return self.encoder