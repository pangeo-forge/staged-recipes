from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

regs = ["GS", "GE", "MD"]


def gen_urls(reg, season=None, datatype=None, grid=False):
    """

    Parameters
    ----------
    season : str
        One of "fma" or "aso".

    Returns
    -------
    A list of input urls
    """
    base = "ftp://ftp.hycom.org/pub/xbxu/ATLc0.02/SWOT_ADAC/"

    if not grid:
        assert datatype, "If `grid=False`, `datatype` must be one of `surf` or `int`."
        months = ["Feb", "Mar", "Apr"] if season == "fma" else ["Aug", "Sep", "Oct"]
        freq = "hourly" if datatype == "surf" else "daily"
        urls = [base + f"HYCOM50_E043_{month}_{reg}_{freq}.nc" for month in months]
    else:
        urls = [
            base + f"HYCOM50_grid_{reg}.nc",
        ]

    return urls


def create_recipes(datatype, season, regs=regs):
    """
    Parameters
    ----------
    datatype : str
        One of `surf` or `int`.
    season : str
        One of "fma" or "aso".
    regs : list of strs
        A list of valid regions.

    Returns
    -------
    A dictionary of recipes.
    """
    patterns = {
        f"HYCOM50/Region{i+1:02}_{reg}/{datatype}/{season}": pattern_from_file_sequence(
            gen_urls(season=season, reg=reg, datatype=datatype), concat_dim="time",
        )
        for i, reg in zip(range(3), regs)
    }
    recipes = {
        list(patterns)[i]: (
            XarrayZarrRecipe(patterns[list(patterns)[i]], target_chunks={"time": 24},)
        )
        for i in range(3)
    }
    return recipes


grid_patterns = {
    f"HYCOM50/Region{i+1:02}_{reg}/grid": (
        pattern_from_file_sequence(gen_urls(reg, grid=True), concat_dim="time", nitems_per_file=1,)
    )
    for i, reg in zip(range(3), regs)
}

grid_recipes = {
    list(grid_patterns)[i]: XarrayZarrRecipe(grid_patterns[list(grid_patterns)[i]],)
    for i in range(3)
}

recipes = {
    **create_recipes("surf", "fma"),
    **create_recipes("surf", "aso"),
    **create_recipes("int", "fma"),
    **create_recipes("int", "aso"),
    **grid_recipes,
}
