class Process:

    def __init__(self, ID, function_list):
        self.ID = ID
        self.function_list = function_list

    def execute(self, df):
        for function in self.function_list:
            df = function(df)
        return df
    
        