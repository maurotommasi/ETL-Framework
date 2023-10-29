# SAMPLE FOR WEB SCRAPING
# In this case the mapping file example to follow is: mapping_source_html.json, inside we have
# {
#     "mapping": ["h1", "h2", "h3", "h4", "h5", "h6", "p", "a", "img"]
# }
# Inside the list we can put the list of tags we want to fetch
# This file will be passed to the encoder

from ETL import ETL

# Inizialize main istance ETL

ETL_istance = ETL("Process ETL 1")

newspaper_websites = [
    "https://www.nytimes.com",
    "https://www.theguardian.com",
    "https://www.wsj.com",
    "https://www.washingtonpost.com",
    "https://www.usatoday.com",
    "https://www.latimes.com",
    "https://www.chicagotribune.com",
    "https://www.boston.com",
    "https://www.independent.co.uk",
    "https://www.theaustralian.com.au"
]

# Initialize sub istances

source = ETL_istance.Source('html') # html is the name of the Source

encoder = source.Encoder()
data_source = source.DataSource()

# Set encoder

encoder.set_html(mapping_url="mapping/mapping_source_html.json")

# Create Save Object - This will help us to save/upload the info as specified in the dictionary
# Each extraction has his own Save Object Dictionary (SOD)

save_obj = {
    'type': 'json',
    'path': "data.json"
}

for url in newspaper_websites:
    # Set Data Source Connection It can be an URL, a MYSQL Connection and so on
    data_source.set_url(url)

    # Set Source Object (Data Source + Enconder)
    source.set_data_source(data_source, encoder)

    # Extract and Load the Information in a queue of threads. Every Extraction is a indipendent thread
    extraction = ETL.Extract(name=url,source=source)
    extraction.queue(save_obj, 3600)

# Here is the last piece of code that has to be run
# This will allow to create N threads based on how many extraction were created
# Is it possible to add multiple extractions from multiple ETL istances
# Example:
# ETL_istance_1 => Web scraping
# ETL_istance_2 => Sync data from Google Cloud
# ETL_istance_3 => Sync data from AWS EC2
# This code will start the ETL process, be aware about IP blovking a request limits by the provider 

ETL_istance.Utils().runThreads([extraction])