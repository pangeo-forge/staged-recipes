import pandas as pd

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


def create_dates(datatype, season):
    """Create dates for recipes

    Parameters
    ----------
    datatype : str
        One of "surf" or "int". Determines date range frequency.
    season : str
        One of "aso" (Aug, Sept, Oct) or "fma" (Feb, Mar, Apr)

    Returns
    -------
    A pandas.data_range
    """
    freq = "5D" if datatype == "surf" else "1D"

    start_fmt = "2009-{d}"
    fma_start = start_fmt.format(d="01-28") if datatype == "surf" else start_fmt.format(d="02-01")

    stop_fmt = "2008-10-{d}"
    aso_stop = stop_fmt.format(d="30") if datatype == "surf" else stop_fmt.format(d="31")
    stop_fmt = "2009-04-{d}"
    fma_stop = stop_fmt.format(d="28") if datatype == "surf" else stop_fmt.format(d="30")

    start_stop = ["2008-08-01", aso_stop] if season == "aso" else [fma_start, fma_stop]

    return pd.date_range(*start_stop, freq=freq)


def create_urls(pattern, datatype, reg, season):
    """Create a list of input urls

    Parameters
    ----------
    pattern : str
        A format string
    datatype : str
        One of "surf" or "int". Passed to create_dates to determine date range frequency.
    reg : str
        A geographic region

    Returns
    -------
    A list of urls
    """
    input_urls = [
        pattern.format(reg=reg, yymmdd=day.strftime("%Y-%m-%d"))
        for day in create_dates(datatype=datatype, season=season)
    ]
    return input_urls


def create_recipes(datatype, season, pattern):
    """Create a dictionary of recipes

    Parameters
    ----------
    datatype : str
        One of "surf" or "int". Passed to create_dates to determine date range frequency.
    season : str
        One of "aso" (Aug, Sept, Oct) or "fma" (Feb, Mar, Apr)
    pattern : str
        A format string

    Returns
    -------
    A dictionary of recipes
    """
    time_dim_length = 120 if datatype == "surf" else 1
    target_chunks = {"time": 24} if datatype == "surf" else {"s_rho": 2}

    patterns = {
        f"GIGATL/Region{i:02}/{datatype}/{season}": (
            pattern_from_file_sequence(
                create_urls(pattern=pattern, datatype=datatype, reg=i, season=season,),
                concat_dim="time",
                nitems_per_file=time_dim_length,
            )
        )
        for i in range(1, 3)
    }
    recipes = {
        list(patterns)[i]: (
            XarrayZarrRecipe(patterns[list(patterns)[i]], target_chunks=target_chunks,)
        )
        for i in range(2)
    }
    return recipes


surf_pattern = "ftp://eftp.ifremer.fr/SWOT/SURF/gigatl1_1h_tides_surf_avg_{reg:1d}_{yymmdd}.nc"

int_pattern = "ftp://eftp.ifremer.fr/SWOT/3D/gigatl1_1h_tides_region_{reg:02d}_{yymmdd}.nc"

recipes = {
    **create_recipes("surf", "aso", surf_pattern),
    **create_recipes("surf", "fma", surf_pattern),
    **create_recipes("int", "aso", int_pattern),
    **create_recipes("int", "fma", int_pattern),
}
