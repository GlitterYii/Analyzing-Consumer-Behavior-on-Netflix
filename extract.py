import os
import pandas as pd
from datetime import datetime

path = "https://github.com/GlitterYii/Netflix/netflix_titles"
data = pd.read_csv(path)
print(data.head())

#extract file and store raw data to floder
def extract(file_url):
    #find path and create floder at desktop
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    raw_dir = os.path.join(desktop_path, "data", "raw")
    os.makedirs(raw_dir, exist_ok=True)

    extracted_data = pd.read_csv(file_url)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = os.path.join(raw_dir, f"netflix_titles_{timestamp}.csv")

    extracted_data.to_csv(file_path, index=False)

    return file_path

def transform(filename):

    return transform

def load(filename):

    return load

def clean_data(file_extrct):
    cleaned = file_extrct[""]
    return cleaned