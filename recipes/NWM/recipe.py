# Author: Norland Raphael Hagen @norlandrhagen 11-01-2022
# Pangeo-Forge recipe for National Water Model - Short Range Forecast  # noqa: E501
# Heavily adapted from Kerchunk example written by Rich Signell (USGS) @rsignell-usgs. https://gist.github.com/rsignell-usgs/ef435a53ac530a2843ce7e1d59f96e22 # noqa: E501

import os

import fsspec

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

# Create fsspec aws filesystem
fs = fsspec.filesystem('s3', anon=True, skip_instance_cache=True)
flist = fs.glob('noaa-nwm-pds/nwm.*/short_range/nwm.*.short_range.channel_rt.f001.conus.nc')

# Join the "best time series" from past forecasts with the latest forecast
# Remove the first day of data since this is a rolling collection and
# we don't want to be trying to access files that soon will be removed.
# & Use all the files from the last forecast cycle

last_dir = f'{os.path.dirname(flist[-1])}'
last_file = os.path.basename(flist[-1]).split('.')
last_files = fs.glob(
    f'{last_dir}/{last_file[0]}.{last_file[1]}.{last_file[2]}.channel_rt.*.conus.nc'
)

# Skip the first of the last_files since it's a duplicate
flist.extend(last_files[1:])

# Append s3 prefix
urls = ['s3://' + f for f in flist]

# Create filepattern from urls
pattern = pattern_from_file_sequence(urls, 'time')

# Create HDFReference recipe from pattern
recipe = HDFReferenceRecipe(pattern, netcdf_storage_options={'anon': True})
