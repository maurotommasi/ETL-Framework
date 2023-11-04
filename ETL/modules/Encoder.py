from .Utils import Utils

class Encoder:

    def __init__(self, ID_encoder, encoder):
        self.ID_encoder = ID_encoder
        self.encoder = encoder

    def get_encoder(self):
        return self.encoder
    
class EncoderConfig:

    def __init__(self):
        None

    def html_encoder_config(self, tags, tags_map):
        if len(tags) == len(tags_map):
            mapping = {}
            index = 0
            for tag in tags:
                mapping[tag] = tags[index]
                index += 1
            return {
                "encoder_type": "html",
                "mapping": mapping
            }
    
    def mysql_encoder_config(self, cols_source, cols_loader):
            mapping = {}
            index = 0
            for col_soure in cols_source:
                mapping[col_soure] = cols_loader[index]
                index += 1
            return {
                "encoder_type": "query",
                "mapping": mapping
            }
    
    def custom_encoder_config(self, cols_source, cols_loader):
            mapping = {}
            index = 0
            for col_soure in cols_source:
                mapping[col_soure] = cols_loader[index]
                index += 1
            return {
                "encoder_type": "custom",
                "mapping": mapping
            }