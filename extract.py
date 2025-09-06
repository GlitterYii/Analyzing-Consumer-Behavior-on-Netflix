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
    #Cleaning data

    #Check data missing & duplicate
    print(f"Check data missing \n{df.isnull().sum()}")
    print(f"Check data duplicated = {df.duplicated().sum()}")

    #Drop duplicates
    df.drop_duplicates(inplace=True)

    #Check data type
    print(f"Chaek data types \n{df.dtypes}")

    # Change dtype
    df["date_added"] = pd.to_datetime(df["date_added"], errors="coerce")
    df["type"] = df["type"].astype("category")
    df["release_yesr"] = df["release_year"].astype(int)
    df["rating"] = df["rating"].astype("category")


    # Process duration &  Drop original duration column correctly
    df['duration'] = df['duration'].astype(str)
    df['duration_int'] = df['duration'].str.extract(r'(\d+)')[0].astype('Int64')
    df['duration_unit'] = df['duration'].str.extract(r'([a-zA-Z ]+)')[0].str.strip()
    df['duration_unit'] = df['duration_unit'].replace({'min': 'minutes', 'Season': 'seasons', 'Seasons': 'seasons'})
    df.drop(columns=['duration'], inplace=True) 
    
    # Split into list
    for i in ['cast', 'listed_in', 'director']:
        df[i] = df[i].str.split(', ')

    print(f"Data types \n{df.dtypes}")
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