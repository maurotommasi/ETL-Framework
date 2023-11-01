
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time

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
        while True:
            try:
                print(f"Start extraction for {self.ID}")
                # Send a GET request to the URL
                response = requests.get(self.data_source.get_data_source())
                tags_to_extract = self.data_source.get_encoder().get_encoder_mapping()['encoder_mapping']['tags']
                # Check if the request was successful
                if response.status_code == 200:
                    # Parse the HTML content of the page
                    try:
                        self.data = self.__scraping(response, tags_to_extract)
                        return True
                    except Exception as e:
                        print("Error:", e)
                        with open("./log.txt", 'a+') as file:
                            file.write(str(datetime.now()) +  ' ' + str(e) + "\n")
                        self.data = {}
                else:
                    print("Error: Unable to fetch the webpage. Status code:", response.status_code)
                    with open("./log.txt", 'a+') as file:
                            file.write(str(datetime.now()) +  ' ' + str(e) + "\n")
            except Exception as e:
                print("Error:", e)
    
    def __scraping(self, response, tags_to_extract, save_obj):
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
        return extracted_text