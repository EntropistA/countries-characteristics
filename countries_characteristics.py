import os

import pandas as pd

DEBUG = True

def binary_search():
    pass

# Countries data files have number index from 0, tehn columns: country, score, rank

class CountriesData:
    
    
    def __init__(self):
        self.country_key = "country"

        self.countries_df = self.read_data("country_code_capital_location_continent.parquet")
        
        self.happiness = self.read_data("happiness_2020.parquet")
        
        self.countries_different_data = [
            self.happiness,
        ]
        
        if DEBUG:
            self.debug()
    
    def read_data(self, filename: str) -> pd.DataFrame:
        return pd.read_parquet(filename, header=0)
    
    def list_countries_data_file_names(self) -> [pd.DataFrame]:
        return [f for f in os.listdir() if f.endswith(".parquet") and f != self.countries]
    
    def assert_key_present(self, df):
        if self.country_key not in self.countries_df.columns:
            raise KeyError(f"Key: {self.country_key} not present in countries file columns: {self.countries_df.columns}")
        for df in self.countries_different_data:
            if self.country_key not in df.columns:
                raise KeyError(f"Key: {self.country_key} not present in countries data columns: {df.columns}")
    
    def assert_country_name_match(self):
        countries = self.countries_df["country"].tolist()
        
        for df in self.countries_different_data:
            for country_name in df["country"].tolist():
                if country_name not in countries:
                    raise KeyError(f"{country_name} not present in countries file")
            
    def debug(self):
        self.assert_key_present()
        self.assert_country_name_match()
        
files = CountriesData()
print(files.countries)
print(files.countries_data)