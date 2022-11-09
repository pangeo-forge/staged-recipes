import pandas as pd
import s3fs
import xarray as xr


fs = s3fs.S3FileSystem(token='anom')
file_url = 's3://noaa-cdr-sea-surface-temp-whoi-pds/data/1988/SEAFLUX-OSB-CDR_V02R00_SST_D19880101_C20160820.nc'
ds = xr.open_dataset(fs.open(file_url), engine='h5netcdf')
print(ds)

# dates = pd.date_range('1988-01-01', '2022-11-08', freq='D')
# # print the first 4 dates
# print("first four dates")
# print(dates[:4])

# # print the last 4 dates
# print("last four dates")
# print(dates[-4:])

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

start_date = "1988-01-01"


def format_function(time):
    base = pd.Timestamp(start_date)
    day = base + pd.Timedelta(days=time)
    input_url_pattern = (
        "s3://noaa-cdr-sea-surface-temp-whoi-pds/data/{day:%Y}"
        "/SEAFLUX-OSB-CDR_V02R00_SST_D{day:%Y%m%d}_C20160820.nc" #will only work for those files created in 2016...
    )
    return input_url_pattern.format(day=day)


dates = pd.date_range(start_date, "2022-11-08", freq="D")
pattern = FilePattern(format_function, ConcatDim("time", range(len(dates)), 1))
print(pattern)
recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=20, cache_inputs=True)
print(recipe)

from pangeo_forge_recipes.recipes import setup_logging
setup_logging()

# how many inputs does the recipe have?
all_inputs = list(recipe.iter_inputs())
print(len(all_inputs))

# and how many chunks?
all_chunks = list(recipe.iter_chunks())
print(len(all_chunks))

