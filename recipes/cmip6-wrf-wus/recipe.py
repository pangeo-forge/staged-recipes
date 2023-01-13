from pangeo_forge_recipes.recipes import HDFReferenceRecipe, setup_logging
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
import pandas as pd
import xarray as xr
import fsspec

URL_FORMAT = ('s3://wrf-cmip6-noversioning/downscaled_products/gcm/cesm2_r11i1p1f1_ssp370/hourly/{Time:%Y}/d02/auxhist_d01_{Time:%Y-%m-%d_%X}')

times = pd.date_range('2014-09-01', '2100-08-31 23:00', freq='H')

time_concat_dim = ConcatDim('Time', times)

def make_url(Time):
    return URL_FORMAT.format(Time=Time)

pattern = FilePattern(make_url, time_concat_dim)

recipe = HDFReferenceRecipe(pattern, 
                            concat_dims=['Time'], coo_map={'Time': [str(time) for time in times]},
                            preprocess=None)
