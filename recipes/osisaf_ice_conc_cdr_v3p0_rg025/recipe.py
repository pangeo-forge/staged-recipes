import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

dates = pd.date_range('1989-01-01', '2021-12-31', freq='D')

missing_dates = [
    pd.Timestamp(1990,8,13), pd.Timestamp(1990,8,25), pd.Timestamp(1990,8,26),
    pd.Timestamp(1990,10,21), pd.Timestamp(1990,10,22), pd.Timestamp(1990,10,26), 
    pd.Timestamp(1990,10,27), pd.Timestamp(1990,10,28), pd.Timestamp(1990,12,21), 
    pd.Timestamp(1990,12,22), pd.Timestamp(1990,12,23), pd.Timestamp(1990,12,24), 
    pd.Timestamp(1990,12,25), pd.Timestamp(1990,12,26), pd.Timestamp(2000,12,1),
    pd.Timestamp(2021, 2, 20)
    ]

# Drop missing dates
dates = dates.drop(missing_dates)

URL_FORMAT = (
    "https://g-08c618.7a577b.6fbd.data.globus.org/"
    "ice_conc_r1440x720_{interim}cdr-v3p0_{time:%Y%m%d}1200.nc"
)

def make_url(time):
    if time.year <= 2020: 
        return URL_FORMAT.format(time=time, interim='')
    if time.year >  2020: 
        return URL_FORMAT.format(time=time, interim='i')

time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)
pattern = FilePattern(make_url, time_concat_dim)

recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=5)
