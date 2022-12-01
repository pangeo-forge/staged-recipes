import fsspec

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes.reference_hdf_zarr import HDFReferenceRecipe

fs = fsspec.filesystem(
    's3', anon=True, client_kwargs={'endpoint_url': 'https://mghp.osn.xsede.org'}
)

all_paths = sorted(fs.glob('s3://rsignellbucket1/LiveOcean/*.nc'))

pattern = pattern_from_file_sequence(['s3://' + path for path in all_paths], 'ocean_time')


recipe = HDFReferenceRecipe(
    pattern,
    netcdf_storage_options={
        'anon': True,
        'client_kwargs': {'endpoint_url': 'https://mghp.osn.xsede.org'},
    },
    identical_dims=['lat_psi', 'lat_rho', 'lat_u', 'lat_v', 'lon_psi', 'lon_rho', 'lon_u', 'lon_v'],
)
