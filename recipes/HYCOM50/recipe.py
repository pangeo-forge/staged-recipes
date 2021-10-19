from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

regs = ["GS", "GE", "MD"]


def gen_urls(reg, season=None, datatype=None, grid=False, velocity=False):
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
        velocity_suffix = "_wvel" if velocity else ""
        urls = [base + f"HYCOM50_E043_{month}_{reg}_{freq}{velocity_suffix}.nc" for month in months]
    else:
        urls = [
            base + f"HYCOM50_grid_{reg}.nc",
        ]

    return urls


def create_recipes(datatype, season, regs=regs, velocity=False):
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
    velocity_suffix = "_wvel" if velocity else ""
    patterns = {
        f"HYCOM50/Region{i+1:02}_{reg}/{datatype}{velocity_suffix}/{season}": pattern_from_file_sequence(
            gen_urls(season=season, reg=reg, datatype=datatype, velocity=velocity,),
            concat_dim="time",
        )
        for i, reg in zip(range(3), regs)
    }
    subset_inputs = {"time": 3} if velocity else {}
    target_chunks = {"time": 4} if velocity else {"time": 24}
    recipes = {
        list(patterns)[i]: (
            XarrayZarrRecipe(
                patterns[list(patterns)[i]],
                target_chunks=target_chunks,
                subset_inputs=subset_inputs,
            )
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
    **create_recipes("int", "fma", velocity=True),
    **create_recipes("int", "aso", velocity=True),
    **grid_recipes,
}
