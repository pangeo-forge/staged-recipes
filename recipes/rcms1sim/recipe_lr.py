from datetime import date, timedelta

import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# 25km products have 7-day window
rcms1sim_lr_tdelta = timedelta(days=6)

input_url_pattern = (
    'https://crd-data-donnees-rdc.ec.gc.ca/CPS/products/RCMS1SIM/RCMS1SIM_25000m_EASE1/'
    '{yyyy}/RCMS1SIM_{syyyymmdd}_{eyyyymmdd}_EASE1_v1.0.nc'
)

# products are generated and published daily with 2-day lag
latest = date.today() - timedelta(days=2)
dates = pd.date_range('2020-03-04', latest, freq='1D')


def make_url(time):
    return input_url_pattern.format(
        yyyy=(time + rcms1sim_lr_tdelta).year,
        syyyymmdd=time.strftime('%Y%m%d'),
        eyyyymmdd=(time + rcms1sim_lr_tdelta).strftime('%Y%m%d'),
    )


pattern = FilePattern(make_url, ConcatDim(name='time', keys=dates, nitems_per_file=1))
# 20 files per chunk seems good
recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=20)
