import os
import re
import time
import pandas as pd
import duckdb
from constants import DB_PATH, SOURCE_PATH

conn = duckdb.connect(str(DB_PATH))

def poke_for_files(source_path: str, file_pattern):
    """
    This function will check the inputted path for files matching a certain pattern
    It will poke every 5 minutes 5 times if it does not find anything
    """
    poke = 0
    while poke < 5:
        if os.listdir(source_path):
            print("Files found")
            #print(os.listdir(source_path))
            matching_files = [
                f for f in os.listdir(source_path)
                if re.match(file_pattern, f)
            ]
            print(matching_files)
            return matching_files
        else:
            poke = poke + 1
            print("No files found. Retrying in 5 minutes")
            time.sleep(300)
    return


def get_matching_file_path(source_path, file_pattern):
    """
    Gets the full file path for any file matching a specified pattern in the specified path
    """
    all_files = os.listdir(source_path)
    matching_files = [f for f in all_files if re.match(file_pattern, f)]
    if len(matching_files) > 1:
        print("More than one file found. Please check the source path.")
        return None
    if len(matching_files) == 0:
        print("No files found. Please check the source path.")
        return None
    else:
        return os.path.join(SOURCE_PATH, matching_files[0])

def create_dataframe_from_csv(file_path):
    """
    This function creates a pandas DataFrame from a CSV file.
    """
    return pd.read_csv(file_path)

def rename_dataframe_cols(df, new_col_names):
    """
    This function renames the columns of a pandas DataFrame.
    """
    df.columns = new_col_names
    return df


