from datetime import datetime

class Control:

    def __init__(self, ID_control, pause, pause_if_error, start_from = datetime.now()):
        self.ID = ID_control
        self.pause = pause
        self.pause_if_error = pause_if_error
        self.start_from = start_from

    def set_pause(self, seconds):
        self.pause = seconds

    def set_pause_if_error(self, seconds):
        self.pause_if_error = seconds

    def set_start_from(self, start_from):
        self.start_from = start_from