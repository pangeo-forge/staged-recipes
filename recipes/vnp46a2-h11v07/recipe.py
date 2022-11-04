"""
A recipe to move VNP46A2 to a cloud analysis ready format.
Based off briannapagan's GPM IMERG recipe.
"""

import datetime

from pangeo_forge_recipes.patterns import FilePattern, ConcatDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

from cmr import GranuleQuery
import pandas as pd
import xarray as xr

# Query VNP46A2 dataset files at h11v07
api = GranuleQuery()
vnp_api = (
    api.short_name("VNP46A2")
    .point(-66, 18) # Cerca de Puerto Rico (h11v07)
)
granules = vnp_api.get_all()

# Extract the link corresponding to each file
downloadable_urls = []
for g in granules:
    for link in (g['links']):

        if link['rel'] == 'http://esipfed.org/ns/fedsearch/1.1/data#':
            # print('adding url: ' + link['href'])
            if not ('h11v07' in link['href']):
                continue # Double-checking we only capture h11v07
            downloadable_urls.append(link['href'])
            break

print(f"{len(downloadable_urls)} urls")


# Dictionaries containing the same information about each granule,
# they just vary what variable you use as key to access them.
vnp_date_dict = dict()  # Use granule date as key
href_date_dict = dict() # Granule download link as key

vnp_dates = [] # List of granule dates, which is passed to ConcatDim later on

for i in range(len(downloadable_urls)):

    # Update broken links from the CMR for this dataset
    href_orig = downloadable_urls[i]
    href_new = href_orig.replace('https://ladsweb.modaps.eosdis.nasa.gov/archive/',
    'https://ladsweb.modaps.eosdis.nasa.gov/opendap/RemoteResources/laads/'
    )+'.nc4'

    # Convert julian date string to Python date object
    year_julian = '-'.join(href_new.split('/')[-3:-1])
    date_href = datetime.datetime.strptime(year_julian, '%Y-%j').date()

    # Save this info into each dictionary and list
    info_dict = dict(
        i=i,
        href=href_new,
        date=date_href,
        year_julian=year_julian,
    )
    vnp_date_dict[date_href] = info_dict
    href_date_dict[href_new] = info_dict
    vnp_dates.append(date_href)

print('Earliest date:', min(vnp_dates).strftime('%Y-%m-%d'))
print('Latest date:  ', max(vnp_dates).strftime('%Y-%m-%d'))

def make_full_path(date: datetime.date) -> str:
    '''
    For each date, return the URL from the collected dictionary.
    '''
    return vnp_date_dict[date]['href']


# Concatenate files along the date dimension (one day per file)
date_concat_dim = ConcatDim('date', vnp_dates, nitems_per_file=1)

pattern = FilePattern(make_full_path, date_concat_dim)


def add_date_dimension(ds : xr.Dataset, filename : str) -> xr.Dataset:
    '''
    Expand the dimensions of the input dataset to include a date dimension which references that image's collection date.
    '''
    # print('Hello from', filename)
    hn = filename # href_new
    date_href = href_date_dict[hn]['date']
    date_index = pd.DatetimeIndex([date_href])
    date_da = xr.DataArray( date_index, [('date', date_index)] )
    ds = ds.expand_dims(date=date_da)
    return ds

# Recipe!
recipe = XarrayZarrRecipe(pattern, process_input=add_date_dimension)

# -------------------------------------------------------------------
# Only use below for LOCAL testing:

# import os
# from fsspec.implementations.local import LocalFileSystem
# from pangeo_forge_recipes.storage import FSSpecTarget, StorageConfig, CacheFSSpecTarget

# cwd = os.getcwd() # Files are saved relative to working directory

# # Target directory for generated ZARR dataset
# target_fs = LocalFileSystem()
# target_path = os.path.join(cwd, 'my-dataset.zarr')
# target = FSSpecTarget(fs=target_fs, root_path=target_path)

# # Cache directory for files downloaded from provider
# cache_fs = LocalFileSystem()
# cache_path = os.path.join(cwd, 'cache_dir')
# cache_spec = CacheFSSpecTarget(fs=cache_fs, root_path=cache_path)

# # Config recipe to use both target ZARR and cache
# recipe.storage_config = StorageConfig(target, cache_spec)


# from pangeo_forge_recipes.recipes import setup_logging
# setup_logging()

# recipe_pruned = recipe.copy_pruned() # Prune to only download 2 files

# print('Full recipe:  ', recipe.file_pattern)
# print('Pruned recipe:', recipe_pruned.file_pattern)

# run_function = recipe_pruned.to_function()
# run_function() # Run pruned recipe

# # Attempt opening the resulting dataset
# ds = xr.open_zarr(recipe_pruned.target_mapper, consolidated=True)
# print(ds)
