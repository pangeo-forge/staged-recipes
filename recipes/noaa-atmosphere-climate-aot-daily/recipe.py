from os.path import basename, join

import s3fs

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

url_base = 's3://noaa-cdr-aerosol-optical-thickness-pds/data/daily/'
fs = s3fs.S3FileSystem(anon=True)
years_folders = fs.ls(join(url_base))
years = list(map(lambda x: basename(x), years_folders))
file_list = []
for year in years:
    file_list += sorted(
        filter(
            lambda x: x.endswith('.nc'),
            map(lambda x: 's3://' + x, fs.ls(join(url_base, str(year)), detail=False)),
        )
    )

pattern = pattern_from_file_sequence(file_list, 'time', nitems_per_file=1)

recipe = HDFReferenceRecipe(pattern, netcdf_storage_options={'anon': True})
