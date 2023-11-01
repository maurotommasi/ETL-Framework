class Transformation:

    def __init__(self):
        None

    def scraping_to_pivot_table(self, df):
        print("Running pivot_table transformation.")
        df = df.drop_duplicates(subset=['original_tag', 'text', 'url', 'date_time', 'ingestion_ID'])
        pivoted_df = df.pivot(index=['text', 'url', 'date_time', 'ingestion_ID'], columns='mapping_tag', values='text').reset_index()
        pivoted_df.columns.name = None
        pivoted_df.fillna('', inplace=True)
        return pivoted_df