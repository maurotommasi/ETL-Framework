
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import pandas as pd

class Ingestion():

    def __init__(self, ID):
        self.ID = ID
        self.data_source = None
        self.data = {}
        self.control = None

    def set_data_source(self, data_source):
        self.data_source = data_source

    def set_control(self, control):
        self.control = control

    def extract_text_from_website(self):
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
                    columns = ["original_tag", "mapping_tag", "text", "url", "date_time", "ingestion_ID"]   
                    self.data = pd.DataFrame(data=self.__scraping(response, tags_to_extract, mapping),columns=columns)
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
    
    def __scraping(self, response, tags_to_extract, mapping):
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract text based on specified tags
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