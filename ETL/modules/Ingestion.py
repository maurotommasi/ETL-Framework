
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd
import mysql.connector

#############################################
#Insert here all libraries used for ingestion

##############################################


class Ingestion():

    def __init__(self, ID, custom_function = None, feedback = {}):
        self.ID = ID
        self.data_source = None
        self.data = {}
        self.control = None
        self.custom_function = custom_function
        self.feedback = feedback

    def set_data_source(self, data_source):
        self.data_source = data_source

    def set_control(self, control):
        self.control = control


    # Scraping

    def from_html(self):
        try:
            print(f"Start extraction for {self.ID}")
            # Send a GET request to the URL
            response = requests.get(self.data_source.get_data_source()['url'])
            tags_to_extract = self.data_source.get_encoder().get_encoder()['tags']
            mapping = self.data_source.get_encoder().get_encoder()['mapping']
            # Check if the request was successful
            if response.status_code == 200:
                # Parse the HTML content of the page
                try:  
                    soup = BeautifulSoup(response.content, 'html.parser')
                    columns = ["original_tag", "mapping_tag", "text", "url", "date_time", "ingestion_ID"] 
                    self.data = pd.DataFrame(data=self.__scraping(soup, tags_to_extract, mapping),columns=columns)

                except Exception as e:
                    print("Error:", e)
                    with open("./log.txt", 'a+') as file:
                        file.write(str(datetime.now()) +  ' ' + str(e) + "\n")
                    self.data = pd.DataFrame(data={},columns=columns)
            else:
                print("Error: Unable to fetch the webpage. Status code:", response.status_code)
                with open("./log.txt", 'a+') as file:
                        file.write(str(datetime.now()) +  ' ' + str(e) + "\n")
        except Exception as e:
            print("Error:", e)
    
    def __scraping(self, soup, tags_to_extract, mapping):
        extracted_text = []
        for tag in tags_to_extract:
            elements = soup.find_all(tag)
            for element in elements:
                text_content = element.get_text(strip=True)
                if element.name == "a" and element.get("href"):
                    url = element.get("href")
                elif element.name == "img" and element.get("src"):
                    url = element.get("src")
                    text_content = element.get("alt")
                else:
                    url = ""
                extracted_text.append({
                    "original_tag": tag,
                    "mapping_tag": mapping[tag],
                    "text": text_content, 
                    "url": url,
                    "date_time": str(datetime.now()),
                    "ingestion_ID": self.ID   
                    })
        return extracted_text

    def from_query_mysql(self):
        df = {}
        try:
            # Establish a connection to the MySQL database
            host = self.data_source.get_data_source()['host']
            user = self.data_source.get_data_source()['username']
            password = self.data_source.get_data_source()['password']
            database = self.data_source.get_data_source()['database']
            connection = mysql.connector.connect(host=host, user=user, password=password, database=database)
            if connection.is_connected():
                query = self.data_source.get_data_source()['query']
                cursor = connection.cursor()
                cursor.execute(query)
                column_names = [description[0] for description in cursor.description]
                results = cursor.fetchall()
                df = pd.DataFrame(results, columns=column_names)
                cursor.close()
                connection.close()
            else:
                print(f"Not Connected to host: {host} - database: {database}")
        except mysql.connector.Error as err:
            print("Error: ", err)
        self.data = df
    
    def from_custom_function(self):
        self.data, self.feedback = self.custom_function(self.feedback)