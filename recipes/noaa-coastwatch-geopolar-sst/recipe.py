import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


dates = pd.date_range('2002-09-01', '2022-03-20', freq='D')

# The following dates are missing from the dataset, because they were processed
# near real time and not backfilled.
missing_dates = [
    pd.Timestamp(2017, 2, 1), pd.Timestamp(2018, 2, 8), pd.Timestamp(2018, 3, 6), 
    pd.Timestamp(2018, 3, 12), pd.Timestamp(2018, 3, 17), pd.Timestamp(2019, 5, 25), 
    pd.Timestamp(2021, 5, 21)
    ]

# Drop missing dates
dates = dates.drop(missing_dates)

def make_url(time):
    yyyy = time.strftime('%Y')
    yyyymmdd = time.strftime('%Y%m%d')

    # Organization swithes from STAR to OSPO in 2017
    if time.year >= 2017:
        org = 'OSPO'
    else:
        org = 'STAR'

    return (
        'https://coastwatch.noaa.gov/pub/socd2/coastwatch/sst_blended/sst5km/'
        f'night/ghrsst/{yyyy}/{yyyymmdd}000000-{org}-L4_GHRSST-SSTfnd-Geo_Polar'
        '_Blended_Night-GLOB-v02.0-fv01.0.nc'
    )

time_concat_dim = ConcatDim("time", dates, nitems_per_file=1)
pattern = FilePattern(make_url, time_concat_dim)

recipe = XarrayZarrRecipe(
    pattern, 
    target_chunks={'time': 2, 'lat': 1800, 'lon': 7200}
    )
