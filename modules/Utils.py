from datetime import datetime
import os
import json
import threading

class Utils:

    def __init__(self):
        None

    def urlToFolder(self, url):
        return url.replace("https://", "").replace("www.", "_").replace("/", "_").lstrip("_")

    def append_to_file(file_path, content):
        try:
            # Open the file in append mode ('a+')
            with open(file_path, 'a+') as file:
                # Write the content to the file
                file.write(content + "\n")  # Add a newline after the appended content
            print("Content has been successfully appended to the file.")
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
