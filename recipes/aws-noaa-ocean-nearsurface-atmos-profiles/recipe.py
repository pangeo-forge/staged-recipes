from os.path import join
import pandas as pd
import s3fs
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

url_base = "s3://noaa-cdr-ocean-nearsurface-atmos-profiles-pds/data/"
file_list = []
fs = s3fs.S3FileSystem(anon=True)
dates = pd.date_range("1988", "2021", freq="A")


def is_nc(x):
    return x.endswith(".nc")


def add_s3(x):
    return "s3://" + x


file_list = []
for year in dates.year:
    file_list += [add_s3(x) for x in fs.ls(join(url_base, str(year))) if is_nc(x)]
file_list = sorted(file_list)
pattern = pattern_from_file_sequence(file_list, "time", nitems_per_file=8)
recipe = HDFReferenceRecipe(pattern, netcdf_storage_options={"anon": True})
