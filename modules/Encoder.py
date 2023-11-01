from modules.Utils import Utils

class Encoder:

    def __init__(self, ID_encoder, encoder):
        self.ID_encoder = ID_encoder
        self.encoder = encoder

    def get_encoder(self):
        return self.encoder
    