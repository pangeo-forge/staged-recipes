import pandas as pd
from datetime import datetime
from pangeo_forge_recipes.patterns import ConcatDim, MergeDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

start_date = datetime(2022, 3, 3, 0, 0, 0)

def make_url(variable, time):

    index = (time - start_date) / pd.Timedelta(1, 'hour')
    index = f'{index:03.0f}'
    return (
        "https://dd.weather.gc.ca/model_riops/netcdf/forecast/polar_stereographic"
        f"/2d/00/{index}/20220303T00Z_MSC_RIOPS_{variable}_DBS-0.5m_PS5km_P{index}.nc"
    )

variable_merge_dim = MergeDim("variable", ["VOTEMPER", "VOSALINE", "VOZOCRTX", "VOMECRTY"])
dates = pd.date_range('2022-03-03 00:00:00',  periods=84, freq='H')
time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)

pattern = FilePattern(make_url, time_concat_dim, variable_merge_dim)

def preproc(ds):
    ds = ds.drop('polar_stereographic')
    
    return ds

recipe = XarrayZarrRecipe(file_pattern=pattern,  
                          target_chunks={'time': 1, 'xc':450,'yc':410},
                          process_chunk=preproc
                         )
