import netrc
import aiohttp
import numpy as np
from cmr import GranuleQuery
from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from pangeo_forge_recipes.patterns import pattern_from_file_sequence

# Create a file pattern
shortname = 'GPM_3IMERGHH'
files = files_from_cmr(
    shortname=shortname,
    concat_dim='time',
    nitems_per_file=1)

# Test that our dataset works
def testds(store:zarr.storage.FSStore) -> zarr.storage.FSStore:
    import xarray as xr
    ds = xr.open_dataset(store,engine='zarr',chunks={})
    assert ds.title == ('Half-Hourly GPM IMERG Final Precipitation V06 (GPM_3IMERGHH)')
    assert ds.attrs['DOI'] == '10.5067/GPM/IMERG/3B-HH/06'  
    assert len(ds.lat) == 1800
    assert len(ds.lon) == 3600    
    dsvars = ['lat','lon','time',
              'HQobservationTime',
              'HQprecipitation',
              'HQprecipSource',
              'IRkalmanFilterWeight',
              'IRprecipitation',
              'precipitationCal',
              'precipitationUncal',
              'precipitationQualityIndex',
              'probabilityLiquidPrecipitation',
              'randomError']
    for var in dsvars:
        assert var in list(ds.variables)   
    return store

# Create a recipe object
recipe = (
    beam.Create(files.items())
    | OpenURLWithEarthDataLogin()
    | OpenWithXarray(
        file_type=pattern.file_type, 
        xarray_open_kwargs={
            'group':'Grid',
            'drop_variables':['latv','lat_bnds','lonv','lon_bnds','nv','time_bnds',]})
    | StoreToZarr(
        store_name='gpm-imerg-hh.zarr',
        target_chunks={'time':2},
        combine_dims=files.combine_dim_keys)
    | 'Test dataset' >> beam.Map(testds))