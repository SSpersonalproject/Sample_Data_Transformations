import os
import re
import time
from constants import SOURCE_PATH

os.chdir("..")
# We have two file patterns we want to match
file_pattern = pattern = r"(transaction_data.*\.csv$)"


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


poke_for_files(SOURCE_PATH, file_pattern)
