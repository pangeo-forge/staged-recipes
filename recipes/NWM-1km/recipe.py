import fsspec
import kerchunk

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

fs = fsspec.filesystem(
    's3', anon=True
)

all_paths = sorted(fs.glob('noaa-nwm-retrospective-2-1-pds/model_output/2017/*LDAS*'))

pattern = pattern_from_file_sequence(['s3://' + path for path in all_paths], 'time')


recipe = HDFReferenceRecipe(
    pattern,
    netcdf_storage_options={
        'anon': True
    },
    identical_dims=['x','y'], preprocess=kerchunk.combine.drop('reference_time')
)
