from modules.Utils import Utils

class Encoder:

    def __init__(self, ID_encoder, encoder_type, encoder_mapping):
        self.encoder_mapping = encoder_mapping
        self.ID_encoder = ID_encoder
        self.encoder = {
            "encoder_type": encoder_type, # html|raw|json|xml|csv
            "encoder_mapping": encoder_mapping
        }

        self.html = 'html'

    def get_encoder(self):
        return self.encoder
    