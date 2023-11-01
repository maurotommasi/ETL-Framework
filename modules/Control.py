class Control:

    def __init__(self, ID_control, pause = 3600, pause_if_error = 3600):
        self.ID = ID_control
        self.pause = pause
        self.pause_if_error = pause_if_error

    def set_pause(self, seconds):
        self.pause = seconds

    def set_pause_if_error(self, seconds):
        self.pause_if_error = seconds

