from modules.Utils import Utils

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
                "tags": tags,
                "mapping": mapping
            }
