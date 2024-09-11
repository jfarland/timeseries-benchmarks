import requests
import urllib

import os
import shutil
import glob

from datetime import datetime
import time 
from distutils.util import strtobool

import datatable as dt 
import pandas as pd 

from bs4 import BeautifulSoup

from tqdm import tqdm

import logging

logging.basicConfig(filename=f'ingest-{round(time.time())}.log', encoding='utf-8', level=logging.INFO)

# Read in Data Frame
df = dt.fread("monash-repository.csv")
df.head()

df = df.to_pandas()

def convert_tsf_to_dataframe(full_file_path_and_name, replace_missing_vals_with="NaN", value_column_name="series_value"):
    '''
    Converts the contents in a .tsf file into a dataframe and returns it along with other meta-data of the dataset.
    
        Parameters:
            full_file_path_and_name (str): complete .tsf file path
            replace_missing_vals_with (str): a term to indicate the missing values in series in the returning dataframe
            value_column_name (str): Any name that is preferred to have as the name of the column containing series values in the returning dataframe
    
        Returns:
            data (pd.DataFrame): load data frame
            frequency (str): time series frequency
            horizon (int): time series forecasting horizon
            missing (bool): whether the dataset contains missing values
            equal (bool): whether the series have equal lengths
    
    '''
    
    col_names = []
    col_types = []
    all_data = {}
    line_count = 0
    frequency = None
    forecast_horizon = None
    contain_missing_values = None
    contain_equal_length = None
    found_data_tag = False
    found_data_section = False
    started_reading_data_section = False

    with open(full_file_path_and_name, "r", encoding="cp1252") as file:
        for line in file:
            # Strip white space from start/end of line
            line = line.strip()

            if line:
                if line.startswith("@"):  # Read meta-data
                    if not line.startswith("@data"):
                        line_content = line.split(" ")
                        if line.startswith("@attribute"):
                            if (
                                len(line_content) != 3
                            ):  # Attributes have both name and type
                                raise Exception("Invalid meta-data specification.")

                            col_names.append(line_content[1])
                            col_types.append(line_content[2])
                        else:
                            if (
                                len(line_content) != 2
                            ):  # Other meta-data have only values
                                raise Exception("Invalid meta-data specification.")

                            if line.startswith("@frequency"):
                                frequency = line_content[1]
                            elif line.startswith("@horizon"):
                                forecast_horizon = int(line_content[1])
                            elif line.startswith("@missing"):
                                contain_missing_values = bool(
                                    strtobool(line_content[1])
                                )
                            elif line.startswith("@equallength"):
                                contain_equal_length = bool(strtobool(line_content[1]))

                    else:
                        if len(col_names) == 0:
                            raise Exception(
                                "Missing attribute section. Attribute section must come before data."
                            )

                        found_data_tag = True
                elif not line.startswith("#"):
                    if len(col_names) == 0:
                        raise Exception(
                            "Missing attribute section. Attribute section must come before data."
                        )
                    elif not found_data_tag:
                        raise Exception("Missing @data tag.")
                    else:
                        if not started_reading_data_section:
                            started_reading_data_section = True
                            found_data_section = True
                            all_series = []

                            for col in col_names:
                                all_data[col] = []

                        full_info = line.split(":")

                        if len(full_info) != (len(col_names) + 1):
                            raise Exception("Missing attributes/values in series.")

                        series = full_info[len(full_info) - 1]
                        series = series.split(",")

                        if len(series) == 0:
                            raise Exception(
                                "A given series should contains a set of comma separated numeric values. At least one numeric value should be there in a series. Missing values should be indicated with ? symbol"
                            )

                        numeric_series = []

                        for val in series:
                            if val == "?":
                                numeric_series.append(replace_missing_vals_with)
                            else:
                                numeric_series.append(float(val))

                        if numeric_series.count(replace_missing_vals_with) == len(
                            numeric_series
                        ):
                            raise Exception(
                                "All series values are missing. A given series should contains a set of comma separated numeric values. At least one numeric value should be there in a series."
                            )

                        all_series.append(pd.Series(numeric_series).array)

                        for i in range(len(col_names)):
                            att_val = None
                            if col_types[i] == "numeric":
                                att_val = int(full_info[i])
                            elif col_types[i] == "string":
                                att_val = str(full_info[i])
                            elif col_types[i] == "date":
                                att_val = datetime.strptime(
                                    full_info[i], "%Y-%m-%d %H-%M-%S"
                                )
                            else:
                                raise Exception(
                                    "Invalid attribute type."
                                )  # Currently, the code supports only numeric, string and date types. Extend this as required.

                            if att_val is None:
                                raise Exception("Invalid attribute value.")
                            else:
                                all_data[col_names[i]].append(att_val)

                line_count = line_count + 1

        if line_count == 0:
            raise Exception("Empty file.")
        if len(col_names) == 0:
            raise Exception("Missing attribute section.")
        if not found_data_section:
            raise Exception("Missing series information under data section.")

        all_data[value_column_name] = all_series
        loaded_data = pd.DataFrame(all_data)

        return (
            loaded_data,
            frequency,
            forecast_horizon,
            contain_missing_values,
            contain_equal_length,
        )

def parse_monash_df(file):
    ''' Function to Parse a Locally Extracted and Downloaded File'''
    
    loaded_data, frequency, forecast_horizon, contain_missing_values, contain_equal_length = convert_tsf_to_dataframe(file)

    logging.info(f'PARSING FILE: {file}...')
    logging.info(f"IDENTIFIED FREQUENCY: {frequency}...")
    logging.info(f"IDENTIFIED FORECAST HORIZON: {forecast_horizon}")

    parsed_df = pd.DataFrame()

    #freq = frequency
    if frequency == 'yearly':
        freq = 'YS' #year start
    elif frequency == 'quarterly':
        freq = 'QS' #quarter start
    elif frequency == 'monthly':
        freq = 'MS'
    elif frequency == 'daily':
        freq = 'D'


    for index,row in tqdm(loaded_data.iterrows()):
        
        name = row.series_name

        try:
            values = row.series_value.tolist()
            length = len(values)
            start = row.start_timestamp
            ds = pd.date_range(start, periods=length, freq=freq)
            series_df = pd.DataFrame({'unique_id':name,'ds':ds, 'values':values})
        except:
            logging.warning(f'FAILED PARSING TIMESERIES: {name}')
            series_df = pd.DataFrame()
            
        parsed_df = pd.concat([parsed_df, series_df], axis=0)
        
    return parsed_df

# Example Usage
#parse_monash_df("m1_yearly_dataset.tsf").head()

def retrieve_monash_df(url):
    
    # Create Soup
    page = requests.get(url)
    
    # Create Soup
    soup = BeautifulSoup(page.text, 'html.parser')
    
    # Find Download URL from Filename Class
    info = soup.select("[class~=filename]")
    
    # Parse Download Url
    download_url = info[0].get('href')
    
    # Download File
    urllib.request.urlretrieve("https://zenodo.org" + download_url, "tmp.zip")
    
# Example Usage
#retrieve_monash_df(df['URL'][1])

project_path = os.getcwd()
data_path = project_path + '/data'
if not os.path.exists(data_path):
    os.makedirs(data_path)

# limit to Daily, Monthly and Yearly Datasets
sample_df = df[df['Frequency'].isin(['Daily', 'Monthly', 'Yearly'])]

print(dt.Frame(sample_df))

for index, row in tqdm(sample_df.iterrows()):
    
    # Make sure current directory is data directory
    os.chdir(data_path)
    
    # Remove Archive File if it already exists in 
    if os.path.exists(data_path + '/tmp.zip'):
        logging.info(f'DELETING EXISTING ARCHIVE FILE...')
        os.remove(data_path + '/tmp.zip')
        
    # Identify url from mapping file
    url = row.URL
    logging.info(f'RETRIEVING URL: {url}')
    
    # Retrieve the file from the url
    retrieve_monash_df(url)
    
    if not os.path.exists('staging'):
        logging.info("STAGING DIRECTORY NOT FOUND, CREATING NEW ONE...")
        os.makedirs('staging')
    else:
        try:
            logging.info("STAGING DIRECTORY FOUND, CLEANING AND RECREATING...")
            shutil.rmtree('staging')
            os.makedirs('staging')
        except OSError as e:
            logging.warning("Error: %s : %s" % (dest, e.strerror))
        
    # Copy Archive File into Staging Directory
    shutil.copy('tmp.zip', 'staging')
    
    # Go into the directory and unpack
    os.chdir('staging')
        
    shutil.unpack_archive('tmp.zip')
    
    # Find any TSF Files
    result = glob.glob('*.{}'.format('tsf'))
    
    # Find the name of the file 
    local_name = result[0].split(".")[0]
    logging.info(f'DATASET NAME: {local_name}...')
    
    # Parse the DataFrame
    local_df = parse_monash_df(result[0])
    
    # Convert to DataTable
    local_df = dt.Frame(local_df)
    
    os.chdir(data_path)
    
    if not os.path.exists(local_name):
        logging.info("DATASET DIRECTORY NOT FOUND, CREATING NEW ONE...")
        os.makedirs(local_name)
    else:
        try:
            logging.info("DATASET DIRECTORY FOUND, CLEANING AND RECREATING...")
            shutil.rmtree(local_name)
            os.makedirs(local_name)
        except OSError as e:
            logging.warning("Error: %s : %s" % (dest, e.strerror))
        
    local_df.to_csv(local_name + '/' + local_name + '.csv')
    
logging.info("COMPLETED INGEST RUN...")
