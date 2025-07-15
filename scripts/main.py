from data_ingestion import DataIngestion
from data_cleaning import DataCleaning
from ai_agent import AIAgent


# Database configure

DB_USER= "postgres"
DB_PASSWORD = "Alf123@"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "demodb"

DB_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"



# Initailize components
ingestion =DataIngestion(DB_URL)
cleaner = DataCleaning()
ai_agent= AIAgent()

###------------ load and clean CSV data ----------###
df_csv= ingestion.load_csv("sample_data.csv")
if df_csv is not None:
    print("\n Cleaning CSV Data")
    df_csv= cleaner.clean_data(df_csv)
    df_csv= ai_agent.process_data(df_csv)
    print("\n AI Cleaned CSV Data :\n", df_csv)

###------- Load and clean database data -------###
df_excel = ingestion.load_excel("sample_data.xlsx")
if df_excel is not None:
    print("\n Cleaning Excel Data....")
    df_excel= cleaner.clean_data(df_excel)
    df_excel= ai_agent.process_data(df_excel)
    print("\n AI- Cleaned Excel Data:\n", df_excel)

####-------- Load and clean database Data -------####
df_db= ingestion.load_from_databse("SELECT * FROM my_table") # change table name
if df_db is not None:
    print("\n Cleaning database data")
    df_db= cleaner.clean_data(df_db)
    df_db= ai_agent.process_data(df_db)
    print("\n AI Cleaned Database Data: \n", df_db)

#### ----------------- Fetch and clean API Data ---------------------###
# fetch api data
API_URL = "https://jsonplaceholder.typicode.com/posts"
df_api= ingestion.fetch_from_api(API_URL)

if df_api is not None:
    print("\n CLeaning API data")

    # keep only first N rows to avoid token overflow
    df_api= df_api.head(30) # adjust this value based on ur dataset size


    # reduce long text fields before sending to OpenAI
    if "body" in df_api.columns:
        df_api["body"]= df_api["body"].apply(lambda x:x[100]+ "..." if isinstance(x, str) else x)  # limit text length

    df_api= cleaner.clean_data(df_api)
    df_api= ai_agent.process_data(df_api)

    print("\n AI - CLeaned API Data: \n", df_api)
    