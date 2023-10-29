from ETL import ETL

# Inizialize main istance ETL

ETL_istance = ETL("Process ETL 1")

url_list = [
    "https://www.bbc.com/news",
    "https://www.cnn.com",
    "https://www.nytimes.com",
    "https://www.theguardian.com",
    "https://www.reuters.com",
    "https://www.aljazeera.com",
    "https://www.nbcnews.com",
    "https://www.abcnews.go.com",
    "https://www.cbsnews.com",
    "https://www.foxnews.com",
]

url_list2 = [
    "https://www.apnews.com",
    "https://www.npr.org",
    "https://www.usatoday.com",
    "https://www.washingtonpost.com",
    "https://www.time.com",
    "https://www.bloomberg.com",
    "https://www.cnbc.com",
    "https://www.newsweek.com",
    "https://www.independent.co.uk",
    "https://www.huffpost.com"
]

# Initialize sub istances

source = ETL_istance.Source('html')
encoder = source.Encoder()
data_source = source.DataSource()

# Set encoder

encoder.set_html(mapping_url="mapping/mapping_source_html.json")

data = {}

# Fetch Data

save_obj = {
    'type': 'json',
    'path': "data.json"
}

for url in url_list:
    # Set Data Source Connection
    data_source.set_url(url)

    # Set Source Object (data Source + Enconder)
    source.set_data_source(data_source, encoder)

    # Extract and Load the Information. Every Extract is a indipendent thread
    extraction = ETL.Extract(url, source)
    extraction.queue(save_obj, 10)


for url in url_list2:
    # Set Data Source Connection
    data_source.set_url(url)

    # Set Source Object (data Source + Enconder)
    source.set_data_source(data_source, encoder)

    # Extract and Load the Information. Every Extract is a indipendent thread
    extraction2 = ETL.Extract(url, source)
    extraction2.queue(save_obj, 60)

ETL_istance.Utils().runThreads([extraction.getThreads(), extraction2.getThreads()])