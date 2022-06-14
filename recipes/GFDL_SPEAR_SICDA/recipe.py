from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

dates = [str(x) for x in range(1982,2018)]

### Data set 1 ###

def make_url1(time):
    return "ftp://ftp.gfdl.noaa.gov/perm/William.Gregory/SIC_observations_NT/nt_"+time+".ice_daily.ens_mean.nc"

time_concat_dim1 = ConcatDim("time", dates)
pattern1 = FilePattern(make_url1, time_concat_dim1)

recipe1 = XarrayZarrRecipe(pattern1, target_chunks={'time':60})

### Data set 2 ###

def make_url2(time):
    return "ftp://ftp.gfdl.noaa.gov/perm/William.Gregory/SICDA_forecasts_F02/"+time+".ice_daily.ens_mean.nc"

time_concat_dim2 = ConcatDim("time", dates)
pattern2 = FilePattern(make_url2, time_concat_dim2)

recipe2 = XarrayZarrRecipe(pattern2, target_chunks={'time':180,'xT':10,'yT':10})

### Data set 3 ###

def make_url3(record):
    return "ftp://ftp.gfdl.noaa.gov/perm/William.Gregory/SICDA_increments_F02/"+record+".incre_raw.mean.part_size.nc"

time_concat_dim3 = ConcatDim("record", dates)
pattern3 = FilePattern(make_url3, time_concat_dim3)

recipe3 = XarrayZarrRecipe(pattern3, target_chunks={'time':97})

### Data set 4 ###

def make_url4(record):
    return "ftp://ftp.gfdl.noaa.gov/perm/William.Gregory/SICDA_priors_F02/"+record+".input_mean.nc"

time_concat_dim4 = ConcatDim("record", dates)
pattern4 = FilePattern(make_url4, time_concat_dim4)

recipe4 = XarrayZarrRecipe(pattern4, target_chunks={'time':97})

### Data set 5 ###

def make_url5(record):
    return "ftp://ftp.gfdl.noaa.gov/perm/William.Gregory/SICDA_posteriors_F02/"+record+".output_mean.nc"

time_concat_dim5 = ConcatDim("record", dates)
pattern5 = FilePattern(make_url5, time_concat_dim5)

recipe5 = XarrayZarrRecipe(pattern5, target_chunks={'time':97})
