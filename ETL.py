import mysql.connector
import json
import requests
import time
from bs4 import BeautifulSoup
import os
import threading
from datetime import datetime

#pip install requests beautifulsoup4
class ETL:

    def __init__(self, name):
        self.name = name        

    def get_name(self):
        return self.name
    
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
                    "encoder_type": self.html,
                    "encoder_mapping": self.__decode_mapping_json(mapping_url)
                }
            
            def set_plain_text(self):
                self.encoder = self.plain_text
            
            def set_json(self, mapping_url):
                self.encoder = {
                    "encoder_type": self.json,
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
            
        def __init__(self, _source_name = "NO_NAME"):
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

        def __init__(self, name, source):
            self.name = name
            self.source = source
            self.running = True
            self.threads = []

        def stop_extraction(self):
            self.running = False

        def resume_extraction(self):
            self.running = True

        def getThreads(self):
            return self.threads
        
        def queue(self, save_obj, pause_seconds = 3600):
            print(f"Queuing for {self.name}")
            if self.source.get_encoder()['encoder_type'] == 'html':
                extraction_thread = threading.Thread(target=self.extract_text_from_website, args=(save_obj, pause_seconds))
                extraction_thread.start()
                self.threads.append(extraction_thread)

        def extract_text_from_website(self, save_obj, pause_seconds):
            try:
                print(f"Start extraction for {self.name}")
                while self.running:
                    print(f'Extraction "{self.name}"...')
                    # Send a GET request to the URL
                    response = requests.get(self.source.get_data_source())
                    tags_to_extract = self.source.get_encoder()['encoder_mapping']['mapping']
                    # Check if the request was successful
                    if response.status_code == 200:
                        # Parse the HTML content of the page
                        try:
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
                            extracted_text['date_time'] = str(datetime.now())    
                            print(f'Extraction "{self.name}" completed.')
                            print(f'Saving/Uploading "{self.name}"...')
                            self.__save(extracted_text, self.name, save_obj)
                            print(f'"{self.name}" correctly saved/uploaded')
                        except Exception as e:
                            print("Error:", e)
                            with open("./log.txt", 'a+') as file:
                                file.write(str(datetime.now()) +  ' ' + str(e) + "\n")
                        time.sleep(pause_seconds)
                    else:
                        print("Error: Unable to fetch the webpage. Status code:", response.status_code)
                        with open("./log.txt", 'a+') as file:
                                file.write(str(datetime.now()) +  ' ' + str(e) + "\n")
                        return None
            except Exception as e:
                print("Error:", e)
                return None
        
        def __save(self, data, name, saveObj):
            destination_type = saveObj['type']
            if destination_type == 'json':
                path = saveObj['path']
                path = f'istances/{name.replace("https://", "").replace("www.", "_").replace("/", "_").lstrip("_")}/{path}'
                # Save the dictionary into a JSON file
                if not os.path.exists(os.path.dirname(path)):
                    os.makedirs(os.path.dirname(path))
                with open(path, "w") as json_file:
                    json.dump(data, json_file, indent=4)

    class Utils:

        def __init__(self):
            None

        def urlToFolder(self, url):
            return url.replace("https://", "").replace("www.", "_").replace("/", "_").lstrip("_")
       
        def runThreads(self, extraction_list):
            for extraction in extraction_list:
                for thread in extraction.getThreads():
                    try:
                        thread.join()
                    except KeyError as err:
                        print("Error: ", err)
                        self.append_to_file("./log.txt",str(datetime.now()) +  ' ' + str(err))

        def append_to_file(file_path, content):
            try:
                # Open the file in append mode ('a+')
                with open(file_path, 'a+') as file:
                    # Write the content to the file
                    file.write(content + "\n")  # Add a newline after the appended content
                print("Content has been successfully appended to the file.")
            except Exception as e:
                print("Error:", e)