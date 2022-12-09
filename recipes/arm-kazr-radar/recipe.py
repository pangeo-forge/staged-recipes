from pangeo_forge_recipes.patterns import FilePattern, pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from urllib.request import urlopen
import json
from datetime import datetime
import os


def format_time(time):
    """
    Parameters
    ----------
    time: datetime
      Time object you desire to convert

    Returns
    ----------
    time_string: str
      Datetime string ready to input into ARM data query
    Convert
    """
    return time.strftime("%Y-%m-%dT%H:%M:%SZ")

def create_url_path(file, username, password):
    url = f"https://adc.arm.gov/armlive/data/saveData?file={file}&user={username}:{password}"
    return url

def get_list_of_files(datastream, start_time, end_time, username, password):
    start = format_time(start_time)
    end = format_time(end_time)

    url = f"https://adc.arm.gov/armlive/data/query?ds={datastream}&start={start}&end={end}&wt=json&user={username}:{password}"

    # Read the remote json
    response = urlopen(url)
    data_json = json.loads(response.read())
    files = [create_url_path(x, username, password) for x in data_json["files"]]

    return files

start_time = datetime(2022, 6, 1, 0)
end_time = datetime(2022, 6, 2, 0)

# Make sure to access your username and password from here
# https://adc.arm.gov/armlive/livedata/home
username = os.environ("arm_username")
password = os.environ("arm_password")

files = get_list_of_files("houkazrcfrgeM1.a1",
                          start_time=start_time,
                          end_time=end_time,
                          username=username,
                          password=password)

pattern = pattern_from_file_sequence(
     files,
     "time",
     file_type='netcdf4'
 )

recipe = XarrayZarrRecipe(
     pattern,
     target_chunks={"time": 600},
 )
