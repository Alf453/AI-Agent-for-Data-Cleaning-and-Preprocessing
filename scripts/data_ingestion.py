import os
import pandas as pd
import requests
from sqlalchemy import create_engine

DATA_DIR= os.path.join(os.path.dirname(os.path.abspath(__file__)),"../data")

class DataIngestion:
    def __init__(self, db_url=None):
        
        self.engine= create_engine(db_url) if db_url else None


    def load_csv(self,file_name):

        file_path =os.path.join(DATA_DIR, file_name)
        try:
            df= pd.read_csv(file_path)
            print(f"CSV Loaded Successfully: {file_path}")
            return df
        except Exception as e:
            print(f"Error Loading CSV: {e}")    
            return None
        
    def load_excel(self, file_name, sheet_name=0):
        file_path=os.path.join(DATA_DIR, file_name)
        try:
            df= pd.read_excel(file_path, sheet_name=sheet_name)
            print(f"Excel Loaded Successfully: {file_path}")
            return df
        except Exception as e:
            print(f"Error Loading Excel: {e}")
            return None
            

    def conect_database(self, db_url):
        try:
            self.engine= create_engine(db_url)
            print("Database connection successful")

        except Exception as e:
            print(f"Error connecting to database: {e}")

    def load_from_database(self, query):

        if not self.engine:
            print("No Database connection. Call connect_database() first ")
            return None

        try:
            df= pd.read_sql(query, self.engine)
            print("Data Loaded from Database Successfully")
            return df
        except Exception as e:                    
            print(f"Error loading data from database: {e} ")
            return None
        
    def fetch_from_api(self, api_url, params= None):
        """Fetch data from API and return as DataFrame."""
        try:
            response = requests.get(api_url, params= params)
            if response.status_code == 200:
                data= response.json()
                df=pd.DataFrame(data)
                print(f"Data fetched from the api successfully")
                return df
            else:
                print(f"API Request Failed: {response.status_code}")
                return None
        except Exception as e:
            print(f"Error fetching data from API: {e}")
            return None