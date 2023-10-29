import mysql.connector
import json
import requests
import time
from bs4 import BeautifulSoup
import os
import urllib.parse

#pip install requests beautifulsoup4
class ETL:

    def __init__(self, name, pause_seconds = 24 * 3600):
        self.name = name
        self.pause_seconds = pause_seconds

    class Source:

        class DataSource:

            def __init__(self):
                self.data_source = None  # Initialize connection as None here

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
            
        class Encoder:

            def __init__(self):
                self.html = 'html'
                self.plain_text = 'plain_text'
                self.json = 'json'
                self.xml = 'xml'
                self.encoder = None
            def set_html(self, mapping_url):
                self.encoder = {
                    "encoder_type": "json",
                    "encoder_mapping": self.__decode_mapping_json(mapping_url)
                }
            
            def set_plain_text(self):
                self.encoder = self.plain_text
            
            def set_json(self, mapping_url):
                self.encoder = {
                    "encoder_type": "json",
                    "encoder_mapping": self.__decode_mapping_json(mapping_url)
                }
            
            def __decode_mapping_json(self, file_path):
                try:
                    with open(file_path, 'r') as json_file:
                        # Load JSON data from the file
                        return json.load(json_file)
                except FileNotFoundError:
                    print("File not found.")
                except json.JSONDecodeError:
                    print("Invalid JSON format in the file.")
    
            def set_xml(self):
                self.encoder = self.xml
            
            def get_encoder(self):
                return self.encoder
            
        def __init__(self, _source_name):
            self.source_name = _source_name
            self.data_source = None
            self.encoder = None

        def set_data_source(self, data_source = None, encoder = None):
            self.data_source = data_source
            self.encoder = encoder

        def get_data_source(self):
            return self.data_source.get_data_source()
        
        def get_encoder(self):
            return self.encoder.get_encoder()
        
        def get_source_name(self):
            return self.source_name
            
    class Extract():

        def __init__(self, source):
            self.source = source
        
        def extract_text_from_website(self):
            try:
                # Send a GET request to the URL
                response = requests.get(self.source.get_data_source())
                tags_to_extract = self.source.get_encoder()['encoder_mapping']['mapping']
                # Check if the request was successful
                if response.status_code == 200:
                    # Parse the HTML content of the page
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Extract text based on specified tags
                    extracted_text = {}
                    for tag in tags_to_extract:
                        elements = soup.find_all(tag)
                        tag_extracted_text = []
                        for element in elements:
                            text_content = element.get_text(strip=True)
                            # Get URL if available
                            url = None
                            if element.name == "a" and element.get("href"):
                                url = element.get("href")
                            if element.name == "img" and element.get("src"):
                                url = element.get("src")
                                text_content = element.get("alt")
                            # Append the text content and URL to the list
                            tag_extracted_text.append({"text": text_content, "url": url})
                        extracted_text[tag] = tag_extracted_text
                    return extracted_text
                    
                else:
                    print("Error: Unable to fetch the webpage. Status code:", response.status_code)
                    return None
            
            except Exception as e:
                print("Error:", e)
                return None
            
    def save(self, data, saveObj):
        destination_type = saveObj['type']
        if destination_type == 'json':
            path = saveObj['path']
            # Save the dictionary into a JSON file
            if not os.path.exists(os.path.dirname(path)):
                os.makedirs(os.path.dirname(path))
            with open(saveObj['path'], "w") as json_file:
                json.dump(data, json_file, indent=4)

    class Utils:

        def __init__(self):
            None

        def urlToFolder(self, url):
            return url.replace("https://", "").replace("www.", "_").replace("/", "_").lstrip("_")
