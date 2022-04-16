import pandas as pd
from datetime import datetime, date
from pangeo_forge_recipes.patterns import ConcatDim, MergeDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# RIOPS is run every 6 hours (at 00, 06, 12, and 18 UTC).  
# but, for the moment, let's use today's date and assume that when this recipe is being run,
# the 00UTC run for today is already available!
start_date = date.today()
time00 = datetime.min.time()
start_time = datetime.combine(start_date, time00)

def make_url(variable, time):

    yyyymmdd = start_date.strftime("%Y%m%d")
    index = f'{time:03.0f}'
    return (
        "https://dd.weather.gc.ca/model_riops/netcdf/forecast/polar_stereographic"
        f"/2d/00/{index}/{yyyymmdd}T00Z_MSC_RIOPS_{variable}_DBS-0.5m_PS5km_P{index}.nc"
    )

variable_merge_dim = MergeDim("variable", ["VOTEMPER", "VOSALINE", "VOZOCRTX", "VOMECRTY"])

# A RIOPS forecast is every hour for 84 hours
time_concat_dim = ConcatDim("time", range(84), nitems_per_file=1)

pattern = FilePattern(make_url, variable_merge_dim, time_concat_dim)

def process_input(ds, filename):
    ds = ds.drop('polar_stereographic')

    # use an encoding that is valid of hourly data
    units = f'hours since {start_date.strftime("%Y-%m-%d")} 00:00:00' 
    ds.time.encoding = {'units': units,
                        'calendar': 'proleptic_gregorian'}
    
    return ds


recipe = XarrayZarrRecipe(file_pattern=pattern,  
                          target_chunks={'time': 1, 'xc':450,'yc':410},
                          process_input=process_input,
                         )