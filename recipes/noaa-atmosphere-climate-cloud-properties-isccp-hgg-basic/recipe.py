from os.path import basename, join

import s3fs

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

url_base = 's3://noaa-cdr-cloud-properties-isccp-pds/data/isccp-basic/hgg'
fs = s3fs.S3FileSystem(anon=True)
yearmonth_folders = fs.ls(join(url_base))
yearmonths = list(map(lambda x: basename(x), yearmonth_folders))
file_list = []
for yearmonth in yearmonths:
    file_list += sorted(
        filter(
            lambda x: x.endswith('.nc'),
            map(lambda x: 's3://' + x, fs.ls(join(url_base, str(yearmonth)), detail=False)),
        )
    )

pattern = pattern_from_file_sequence(file_list, 'time', nitems_per_file=1)

recipe = HDFReferenceRecipe(pattern, netcdf_storage_options={'anon': True})
