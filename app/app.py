import streamlit as st
import requests
import pandas as pd
import json
from io import StringIO


## Fast API backend URL
FASTAPI_URL= "http://127.0.0.1:8000"


## Stremlit UI COnfiguration
st.set_page_config(page_title= "AI powered Data Cleaning", layout="wide")

# slider data source selection
st.sidebar.header("data source selection")
data_source= st.sidebar.radio(
    "Select Data Source",
    ["CSV/Excel", "Database Query ", "API Data"],
    index=0
)

# main title
st.markdown(
    """ **AI-POWERED DATA CLEANING**
    *Clean your data effortlessly using AI Powered processing!*
    """)

# Handling CSV/Excel upload
if data_source == "CSV/Excel":
     st.subheader("Upload file for cleaning")
     uploaded_file = st.file_uploader("choose a CSV or Excel file", type=["csv", "xlsx"])

     if uploaded_file is not None:
        file_extension = uploaded_file.name.split(".")[-1]
        if file_extension == "csv":
               df = pd.read_csv(uploaded_file)
        else:
               df = pd.read_excel(uploaded_file)

        
        st.write("### Raw Data Preview")
        st.dataframe(df)

        if st.button("Clean Data"):
              files= {"file": (uploaded_file.name, uploaded_file.getvalue())}
              response= requests.post(f"{FASTAPI_URL}/clean_data", files= files)
             
              if response.status_code ==200:
                    st.subheader("Raw API Response (Debugging)")
                    st.json(response.json())


                    # parse cleaned data properly
                    try:
                        cleaned_data_raw = response.json()["cleaned_data"]
                        if isinstance(cleaned_data_raw, str):
                                cleaned_data = pd.DataFrame(json.loads(cleaned_data_raw))
                        else:
                                cleaned_data= pd.DataFrame(cleaned_data_raw)
                        st.subheader("cleaned Data:")
                        st.dataframe(cleaned_data)
                    except Exception as e:
                          st.error(f"Error converting response to DataFrame: {e}")
              else:
                        st.error("Failed to clean data")  

# handling database query
elif data_source == "Databse Query":
      st.subheader("Enter Database Querry")
      db_url = st.text_input("Database Connection URL:", "postgresql://user:password@localhost:5432/db")
      query= st.text_area("Enter SQL Query:", "SELECT * FROM my_table")

      if st.button("Fetch & Clean Data"):
            response = requests.post(f"{FASTAPI_URL}/clean-db", json={"db_url":db_url, "query": query})




            if response.status_code == 200:
                 st.subheader("Raw API Response (debugging)")
                 st.json(response.json())


            try:
                       cleaned_data_raw = response.json()["cleaned_data"]
                       if isinstance(cleaned_data_raw, str):
                             cleaned_data= pd.DataFrame(json.loads(cleaned_data_raw))
                       else:
                             cleaned_data= pd.DataFrame(cleaned_data_raw)

                             st.subheader("cleaned data:")
                             st.dataframe(cleaned_data)
            except Exception as e:
                  st.error(f"Error to fetch/ clean data from database:")

# handlinmg API data

elif data_source == "API Data":
      st.subheader("Fetch data from API")
      api_url = st.text_input("Enter API Endpoint: ", "http://jsonplaceholder.typicode.com/posts")


      if st.button("fetch and clean data"):
            response = requests.posts(f"{FASTAPI_URL}/clean-api", json={"api_url",api_url})

            if response.status_code ==200:
                  st.subheader("raw API Response (debugging)")
                  st.json(response.json())

                  try:
                        cleaned_data_raw = response.json()["cleaned_data"]
                        if isinstance(cleaned_data_raw, str):
                              cleaned_data= pd.DataFrame(json.loads(cleaned_data_raw))
                        else:
                              cleaned_data=pd.DataFrame(cleaned_data_raw)

                        st.subheader("cleaned data:")
                        st.dataframe(cleaned_data)
                  except Exception as e:
                        st.error(f"Error converting response to dataframe:{e}")
                  else:
                        st.error("Failed to fetch/ clean data feom API")


## foter
st.markdown("""---
            
            *Built with **Streamliyt +FASTAPI +AI** for automated data cleaning
            *""")

                              



                        
                  