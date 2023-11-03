from datetime import datetime
import os
import json
import threading
import pickle

class Utils:

    def __init__(self):
        None

    def urlToFolder(self, url):
        return url.replace("https://", "").replace("www.", "_").replace("/", "_").lstrip("_")

    def append_to_file(self, file_path, content):
        try:
            # Open the file in append mode ('a+')
            with open(file_path, 'a+') as file:
                # Write the content to the file
                file.write(content + "\n")  # Add a newline after the appended content
            #print("Content has been successfully appended to the file.")
        except Exception as e:
            print("Error:", e)

    def read_json(self, file_path):
        try:
            with open(file_path, 'r') as json_file:
                # Load JSON data from the file
                return json.load(json_file)
        except FileNotFoundError:
            print("File not found.")
        except json.JSONDecodeError:
            print("Invalid JSON format in the file.")

    def save_obj(self, path, obj):
        with open(path,'wb') as file:
            pickle.dump(obj, file)

    def load_obj(self, path):
        with open(path,'rb') as file:
            return pickle.load(file)
