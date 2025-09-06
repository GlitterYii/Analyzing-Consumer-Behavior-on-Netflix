import os
import pandas as pd
from datetime import datetime

file_name = "netflix_titles.csv"
#extract file and store raw data to floder
def extract(file_name):
    #find path and create floder at desktop
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    raw_dir = os.path.join(desktop_path, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    extracted_data = pd.read_csv(file_name)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(raw_dir, f"netflix_titles_{timestamp}.csv")

    extracted_data.to_csv(file_path, index=False)

    return extracted_data

def transform(df):
    #Cleaning data

    #Check data missing & duplicate
    missing = df.isnull()
    print(f"Data missing \n{missing.sum()}")
    dup = df.duplicated()
    print(f"Data duplicated    {dup.sum()}")

    #check data type
    print(df.dtypes)

    # Change dtype
    df["date_added"] = pd.to_datetime(df["date_added"], errors="coerce")
    df["type"] = df["type"].astype("category")

    # Process duration
    df['duration'] = df['duration'].astype(str)
    df['duration_int'] = df['duration'].str.extract(r'(\d+)')[0].astype(float)
    df['duration_unit'] = df['duration'].str.extract(r'([a-zA-Z ]+)')[0].str.strip()
    df['duration_unit'] = df['duration_unit'].replace("Season", "Seasons")

    # Drop original duration column correctly
    df.drop(columns=['duration'], inplace=True) 

    # Split cast into list
    df['cast'] = df['cast'].str.split(', ')

    print(df.head())

    return df

def load(df,filename):
    df.to_csv(filename)

def pipeline(file_name):
    extract_data = extract(file_name)
    transform_data = transform(extract_data)
    load_data = load(transform_data,f"{file_name}_ETL.csv")

pipeline(file_name)