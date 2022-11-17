import s3fs

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

url_base = 's3://noaa-cdr-sea-surface-temp-whoi-pds/data/'

years = range(1988, 2022)
file_list = []

fs = s3fs.S3FileSystem(anon=True)

for year in years:
    file_list += sorted(
        filter(lambda x: x.endswith('.nc'), fs.ls(url_base + str(year), detail=False))
    )

pattern = pattern_from_file_sequence(file_list, 'time', nitems_per_file=1)

recipe = HDFReferenceRecipe(pattern, netcdf_storage_options={'anon': True})
