# Recipe for WOA 1degree data 
# Data description: https://www.ncei.noaa.gov/sites/default/files/2020-04/woa18documentation.pdf 
import xarray as xr

from pangeo_forge_recipes import patterns
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


def make_url(variable, time):
    # Many cases needed for different variables
    # Here we might not even use all
    if (variable == 'temperature') or (variable == 'salinity'):
        fname =("https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/"
            f"{variable}/decav/1.00/woa18_decav_{variable[0]}{time:02d}_01.nc")
    elif variable == 'density':
        fname = ("https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/"
            f"{variable}/decav/1.00/woa18_decav_I{time:02d}_01.nc")
    elif variable == 'oxygen' or variable == 'AOU' or variable == 'phosphate' or variable == 'nitrate':
        fname = ("https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/"
            f"{variable}/all/1.00/woa18_all_{variable[0]}{time:02d}_01.nc")
    elif variable == 'silicate':
        fname = ("https://www.ncei.noaa.gov/thredds-ocean/fileServer/ncei/woa/"
            f"{variable}/all/1.00/woa18_all_i{time:02d}_01.nc")
    elif variable == 'mld':
        fname = ("https://www.ncei.noaa.gov/data/oceans/woa/WOA18/DATA/"
            f"{variable}/netcdf/decav81B0/1.00/woa18_decav81B0_M02{time:02d}_01.nc")

    return fname


variable_merge_dim = patterns.MergeDim("variable", keys=["temperature", 
                                                         "salinity",
                                                         "density",
                                                         "oxygen", 
                                                         "AOU", 
                                                         #"phosphate", # these following variables do not have data to 1500m in monthly
                                                         #"nitrate", 
                                                         #"silicate", 
                                                         #"mld"
                                                        ])

month_concat_dim = patterns.ConcatDim("time", keys=list(range(1, 13)), nitems_per_file=1)

pattern = patterns.FilePattern(make_url, variable_merge_dim, month_concat_dim)

def fix_encoding_and_attrs(ds, fname):
    ds.time.attrs['calendar'] = '360_day'
    ds = xr.decode_cf(ds)
    ds = ds.set_coords(['crs', 'lat_bnds', 'lon_bnds', 'depth_bnds', 'climatology_bounds'])
    return ds

recipe = XarrayZarrRecipe(
    pattern,
    xarray_open_kwargs={'decode_times': False},
    process_input=fix_encoding_and_attrs,
    target_chunks={"time": 12},
    inputs_per_chunk=12,
)
