from datetime import date, timedelta

import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# 6.25km products have 3-day window
rcms1sim_hr_tdelta = timedelta(days=2)

input_url_pattern = (
    'https://crd-data-donnees-rdc.ec.gc.ca/CPS/products/RCMS1SIM/RCMS1SIM_6250m_EASE2/'
    '{yyyy}/RCMS1SIM_{syyyymmdd}_{eyyyymmdd}_EASE2_v1.0.nc'
)

# products are generated and published daily with 2-day lag
latest = date.today() - timedelta(days=2)
dates = pd.date_range('2020-03-08', latest, freq='1D')


def make_url(time):
    return input_url_pattern.format(
        yyyy=(time + rcms1sim_hr_tdelta).year,
        syyyymmdd=time.strftime('%Y%m%d'),
        eyyyymmdd=(time + rcms1sim_hr_tdelta).strftime('%Y%m%d'),
    )


pattern = FilePattern(make_url, ConcatDim(name='time', keys=dates, nitems_per_file=1))
# each file is pretty big (~88MB)
# v2.0 will likely be a subset of the default 2880x2880 EASE2 extent)
recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=1)
