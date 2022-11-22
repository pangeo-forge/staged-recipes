import os
from os.path import join

import s3fs

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

url_base = 's3://noaa-cdr-sea-surface-temp-whoi-pds/data/'

file_list = []
fs = s3fs.S3FileSystem(anon=True)


def is_nc(x):
    return x.endswith('.nc')


def add_s3(x):
    return 's3://' + x


years_folders = fs.ls(join(url_base))
years = list(map(lambda x: os.path.basename(x), years_folders))

for year in years:
    file_list += sorted(filter(is_nc, map(add_s3, fs.ls(join(url_base, str(year)), detail=False))))
pattern = pattern_from_file_sequence(file_list, 'time', nitems_per_file=1)
recipe = HDFReferenceRecipe(pattern, netcdf_storage_options={'anon': True})
