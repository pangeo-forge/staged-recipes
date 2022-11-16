import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

start_date = '1988-01-01'


def format_function(time):
    base = pd.Timestamp(start_date)
    day = base + pd.Timedelta(days=time)
    input_url_pattern = (
        's3://noaa-cdr-sea-surface-temp-whoi-pds/data/{day:%Y}'
        '/SEAFLUX-OSB-CDR_V02R00_SST_D{day:%Y%m%d}_C*.nc'
    )
    return input_url_pattern.format(day=day)


dates = pd.date_range(start_date, '2022-11-08', freq='D')
pattern = FilePattern(format_function, ConcatDim('time', range(len(dates)), 1))
recipe = HDFReferenceRecipe(pattern, netcdf_storage_options={'anon': True})
