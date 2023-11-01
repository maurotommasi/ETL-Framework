class Process:

    def __init__(self, ID, transformation_function):
        self.ID = ID
        self.transformation_function = transformation_function

    def execute_list(self, function_list, df):
        for function in function_list:
            df = function(df)
        return df
    
    def execute(self, df):
        return self.transformation_function(df)
        