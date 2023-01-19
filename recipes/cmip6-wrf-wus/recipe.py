import functools

import pandas as pd

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import HDFReferenceRecipe

recipe_dict = {
    'cesm2_r11i1p1f1_ssp370': {
        'url_format': (
            'https://wrf-cmip6-noversioning.s3.amazonaws.com/downscaled_products/gcm/'
            'cesm2_r11i1p1f1_ssp370/hourly/{Time:%Y}/d02/auxhist_d01_{Time:%Y-%m-%d_%X}'
        ),
        'times': pd.date_range('2014-09-01', '2100-08-31 23:00', freq='H'),
    }
}


def make_url(Time, URL_FORMAT=None):
    return URL_FORMAT.format(Time=Time)


recipes = {}
for key, value in recipe_dict.items():
    times = value['times']
    time_concat_dim = ConcatDim('Time', times)
    make_url_partial = functools.partial(make_url, URL_FORMAT=value['url_format'])
    pattern = FilePattern(make_url_partial, time_concat_dim)
    recipes[key] = HDFReferenceRecipe(
        pattern,
        concat_dims=['Time'],
        coo_map={'Time': [str(time) for time in times]},
        preprocess=None,
    )
