# ETL (Extract, Transform, Load) Process

The provided Python code defines an ETL (Extract, Transform, Load) process to extract data from a specified data source (URL or MySQL database), transform the data using specified HTML tag mappings, and save the transformed data into a JSON file.

# ETL Class Structure:

## Attributes:
        name: Name of the ETL process.
        pause_seconds: Pause interval between successive runs (default is 24 hours).

## Source Class:

### Attributes:
        source_name: Name of the data source.
        data_source: Instance of DataSource class.
        encoder: Instance of Encoder class.

### Methods:
        set_data_source(data_source, encoder): Set the data source and encoder for the source.
        get_data_source(): Get the data source (URL or MySQL connection).
        get_encoder(): Get the encoder type and mapping.
        get_source_name(): Get the name of the data source.

## DataSource Class:

### Attributes:
        data_source: Holds the URL or MySQL connection object.

### Methods:
        set_url(url): Set the data source as a URL.
        set_mysql(host, username, password, database): Set the data source as a MySQL database connection.
        get_data_source(): Get the data source (URL or MySQL connection).

## Encoder Class:

### Attributes:
        html, plain_text, json, xml: Constants representing encoding types.
        encoder: Holds the selected encoding type and mapping.
### Methods:
        set_html(mapping_url), set_plain_text(), set_json(mapping_url), set_xml(): Set encoding type and mapping.
        __decode_mapping_json(file_path): Private method to decode JSON mapping file.
        get_encoder(): Get the encoding type and mapping.

## Extract Class:

### Attributes:
        source: Instance of Source class.

### Methods:
        extract_text_from_website(): Extract text data from a website based on specified HTML tags and mappings.

## Utils Class:

### Methods:
        urlToFolder(url): Convert a URL to a folder name by removing "https://", "www.", and replacing slashes with underscores.
