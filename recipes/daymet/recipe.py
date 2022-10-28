import datetime

from pangeo_forge_recipes import patterns
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


def make_recipe(region, frequency):
    """
    Make a daymet recipe for given region with given frequency.

    region is "na", "pr" or "hi"
    frequency is "daily", "mon" (monthly) or "ann" (yearly)
    """
    # Aggregate variables available
    AGG_VARIABLES = {'prcp', 'swe', 'tmax', 'tmin', 'vp'}
    # We have a few more variables available daily, in addition to the aggregate ones
    DAILY_VARIABLES = AGG_VARIABLES | {'dayl', 'srad'}

    if frequency in {'mon', 'ann'}:
        # Aggregated data - monthly or annual
        variables = list(AGG_VARIABLES)

        if frequency == 'ann':
            nitems_per_file = 1
            kwargs = dict()
        else:
            nitems_per_file = 12
            kwargs = dict(subset_inputs={'time': 12})

        def format_function(variable, time):
            # https://thredds.daac.ornl.gov/thredds/fileServer/ornldaac/1855/daymet_v4_prcp_monttl_hi_1980.nc
            assert variable in AGG_VARIABLES

            folder = '1852' if frequency == 'ann' else '1855'
            if variable == 'prcp':
                agg = 'ttl'
            else:
                agg = 'avg'
            return f'https://thredds.daac.ornl.gov/thredds/fileServer/ornldaac/{folder}/daymet_v4_{variable}_{frequency}{agg}_{region}_{time:%Y}.nc'

    else:
        variables = list(DAILY_VARIABLES)
        nitems_per_file = 365
        kwargs = dict(subset_inputs={'time': 365})

        def format_function(variable, time):
            assert variable in DAILY_VARIABLES
            # https://thredds.daac.ornl.gov/thredds/fileServer/ornldaac/1840/daymet_v4_daily_hi_dayl_1980.nc
            return f'https://thredds.daac.ornl.gov/thredds/fileServer/ornldaac/1840/daymet_v4_{frequency}_{region}_{variable}_{time:%Y}.nc'

    variable_merge_dim = patterns.MergeDim('variable', keys=variables)

    dates = [datetime.datetime(y, 1, 1) for y in range(1980, 2021)]
    concat_dim = patterns.ConcatDim('time', keys=dates, nitems_per_file=nitems_per_file)

    pattern = patterns.FilePattern(
        format_function(region, frequency), variable_merge_dim, concat_dim
    )

    recipe = XarrayZarrRecipe(pattern, copy_input_to_local_file=True, **kwargs)

    return recipe


regions = ('na', 'hi', 'pr')
frequencies = ('mon', 'ann')

recipes = {}

for region in regions:
    for freq in frequencies:
        id = f'{region}_{freq}'
        recipes[id] = make_recipe(region, freq)
