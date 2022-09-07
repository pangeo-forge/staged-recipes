from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes.xarray_zarr import XarrayZarrRecipe

# C-iTRACE covers a number of proxies
variables = ['d18O']

def make_url(time, variable):
    url = 'https://gdex.ucar.edu/dataset/204_ajahn/file/ctrace.decadal.{variable}.nc'.format(variable=variable)
    return url

# there is only one, but notably ~6 GB file covering the full timeseries
time_concat_dim = ConcatDim("time", [0], nitems_per_file=2200)
pattern = FilePattern(make_url,
                      time_concat_dim,
                      MergeDim(name="variable", keys=variables))


# Create recipe object
# use subset_inputs to make the processing more tractable
recipe = XarrayZarrRecipe(pattern, inputs_per_chunk=1,
                          consolidate_zarr=False,
                          subset_inputs={'time':220},
                          copy_input_to_local_file=True,
                          xarray_open_kwargs={'decode_coords':True})
