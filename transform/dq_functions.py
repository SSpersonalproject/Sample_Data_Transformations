import pandas as pd
import sys

def check_uniqueness(dataframe, unique_key):
    if not all(col in dataframe.columns for col in unique_key):
        raise ValueError("One or more columns in the unique key are not present in the DataFrame.")

    # Calculate the number of duplicate rows based on the unique key
    num_duplicates = dataframe.duplicated(subset=unique_key, keep=False).sum()
    total_rows = len(dataframe)
    duplicate_percentage = (num_duplicates / total_rows) * 100

    if num_duplicates == 0:
        print(f"The combination of columns {unique_key} is unique in the DataFrame.")
        return True
    elif duplicate_percentage < 2:
        print("WARNING: Small number of duplicate records detected. Please review source data.")
    else:
        print("ERROR: Large volume of duplicates found")
        sys.exit(1)  # Exit the program

    return False

import pandas as pd



def check_for_nulls(dataframe, column_list, dataframe_name):
    """
        Takes a dataframe and list of columns as input, and returns
        a dictionary with the number of NULL values for each column
        that contains NULLs.
    """
    if not all(col in dataframe.columns for col in column_list):
        raise ValueError("One or more columns in the input list are not present in the DataFrame.")

    # Count NULL values by column
    null_counts = dataframe[column_list].isnull().sum()

    # Exclude columns without NULLs
    nulls_detected = null_counts[null_counts > 0]

    if nulls_detected.empty:
        print(f"Check passed. No NULLs detected in DataFrame '{dataframe_name}'.")
        return {"dataframe_name": dataframe_name, "nulls": {}}
    else:
        print(f"WARNING: NULL values detected in DataFrame '{dataframe_name}'. Please review source data.")
        return {"dataframe_name": dataframe_name, "nulls": nulls_detected.to_dict()}