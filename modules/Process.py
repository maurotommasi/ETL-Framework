class Process:

    def __init__(self, ID):
        self.ID = ID
        self.ingestion = None
        self.transformation_function = None

    def set_ingestion(self, ingestion):
        self.ingestion = ingestion

    def set_transformation_function(self, transformation_function):
        self.transformation_function = transformation_function

    def execute(self, df):
        return self.transformation_function(df)
        