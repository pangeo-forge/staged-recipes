from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe

url = "{mock_concat}https://www.metoffice.gov.uk/hadobs/hadisst/data/HadISST_sst.nc.gz"

time_concat_dim = ConcatDim("mock_concat", [""], nitems_per_file=1)

def make_full_path(mock_concat):
    return url.format(mock_concat=mock_concat)
filepattern = FilePattern(make_full_path, time_concat_dim, file_type="netcdf3")

recipe = XarrayZarrRecipe(filepattern, copy_input_to_local_file=True)
