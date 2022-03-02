import pandas as pd


class ExcelFileLoaderService:
    def load(self, path):
        # Read excel file as pandas dataframe
        df = pd.read_excel(path)
        df.columns = df.iloc[9]
        # Drop the comments in the excel
        df_dropped = df[10:]
        return df_dropped
