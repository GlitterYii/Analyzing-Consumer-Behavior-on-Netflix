import os
import pandas as pd
from datetime import datetime

file_name = "netflix_titles.csv"
desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")

#extract file and store raw data to floder
def extract(desktop_path,file_name):
    #find path and create floder at desktop
    raw_dir = os.path.join(desktop_path, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    extracted_data = pd.read_csv(file_name)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(raw_dir, f"netflix_titles_{timestamp}.csv")

    extracted_data.to_csv(file_path, index=False)

    return extracted_data

def transform(df):
    # Cleaning data

    # Check for duplicates first, before modifying data types
    print(f"Check data missing \n{df.isnull().sum()}")
    print(f"Check data duplicated = {df.duplicated().sum()}\n")

    # Drop duplicates
    df.drop_duplicates(inplace=True)
    
    # Handle missing values in specific columns
    df['rating'] = df['rating'].fillna("Not Rated")

    # Fill 'director', 'cast', 'country' with 'NaN'
    fill_columns_str = ['director', 'cast', 'country']
    for col in fill_columns_str:
        df[col] = df[col].fillna('NaN')
    
    # Check data type
    print(f"Chaek data types \n{df.dtypes}\n")

    # Change dtype and handle missing values for 'date_added'
    df["date_added"] = pd.to_datetime(df["date_added"], errors="coerce")
    date_added_mode = df["date_added"].mode()[0]
    df["date_added"] = df["date_added"].fillna(date_added_mode)

    # Change dtype and handle missing values for 'release_year'
    df["release_year"] = df["release_year"].astype('Int64')
    release_year_mode = df["release_year"].mode()[0]
    df["release_year"] = df["release_year"].fillna(release_year_mode)

    df["type"] = df["type"].astype("category")
    df["rating"] = df["rating"].astype("category")

    # Process duration & Drop original duration column correctly
    df['duration'] = df['duration'].fillna('0 min')
    df['duration_int'] = df['duration'].str.extract(r'(\d+)')[0].astype('Int64')
    df['duration_int'] = df['duration_int'].fillna(df["duration_int"].mean())
    
    df['duration_unit'] = df['duration'].str.extract(r'([a-zA-Z ]+)')[0].str.strip()
    df['duration_unit'] = df['duration_unit'].replace({'min': 'minutes', 'Season': 'seasons', 'Seasons': 'seasons'})
    df.drop(columns=['duration'], inplace=True) 
    
    # Split into list
    for i in ['cast', 'listed_in', 'director']:
        df[i] = df[i].str.split(', ')

    print(f"Check data missing \n{df.isnull().sum()}")
    print(f"Data types \n{df.dtypes}\n")
    print(df.head())

    return df

def load(desktop_path,df,filename):
    ETL_dir = os.path.join(desktop_path,"data","ETL") 
    os.makedirs(ETL_dir, exist_ok=True)

    file_path = os.path.join(ETL_dir,f"ETL_{filename}")
    df.to_csv(file_path)

def pipeline(file_name):
    extract_data = extract(desktop_path,file_name)
    transform_data = transform(extract_data)
    load_data = load(desktop_path,transform_data,file_name)

pipeline(file_name)