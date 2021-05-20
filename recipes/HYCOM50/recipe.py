from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


def gen_urls(reg, freq=None, grid=False):

    months = ["Feb", "Mar", "Apr", "Aug", "Sep", "Oct"]
    base = "ftp://ftp.hycom.org/pub/xbxu/ATLc0.02/SWOT_ADAC/"

    if grid == False:
        urls = [base + f"HYCOM50_E043_{month}_{reg}_{freq}.nc" for month in months]
    else:
        urls = [
            base + f"HYCOM50_grid_{reg}.nc",
        ]

    return urls


regs = ["GS", "GE", "MD"]

surf_patterns = {
    f"surf_{i+1:02}": pattern_from_file_sequence(gen_urls(reg, "hourly"), concat_dim="time",)
    for i, reg in zip(range(3), regs)
}

surf_recipes = {
    list(surf_patterns)[i]: (
        XarrayZarrRecipe(surf_patterns[list(surf_patterns)[i]], target_chunks={"time": 24},)
    )
    for i in range(3)
}

int_patterns = {
    f"int_{i+1:02}": pattern_from_file_sequence(gen_urls(reg, "daily"), concat_dim="time",)
    for i, reg in zip(range(3), regs)
}

int_recipes = {
    list(int_patterns)[i]: (
        XarrayZarrRecipe(int_patterns[list(int_patterns)[i]], target_chunks={"time": 24},)
    )
    for i in range(3)
}

grid_patterns = {
    f"grid_{i+1:02}": (
        pattern_from_file_sequence(gen_urls(reg, grid=True), concat_dim="time", nitems_per_file=1,)
    ) 
    for i, reg in zip(range(3), regs)
}

grid_recipes = {
    list(grid_patterns)[i]: XarrayZarrRecipe(grid_patterns[list(grid_patterns)[i]],)
    for i in range(3)
}

recipes = {
    **surf_recipes,
    **int_recipes,
    **grid_recipes,
}
