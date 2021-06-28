from pangeo_forge_recipes.patterns import MergeDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


def make_full_path(variable):
    """Returns a valid path to the source files

    Parameters
    ----------
    variable: str
        A string representing each variable
    """
    return f"fill_in_the_path_here_{variable}.nc"


vars = [
    # this should be a list of strings cooresponding to the variable names
    # it will eventually be passed into the `make_full_path` function,
    # to generate the source file paths
]

merge_dim = MergeDim("variable", keys=vars)

pattern = FilePattern(make_full_path, merge_dim)

chunks = {
    # this should be a dictionary mapping the name of the time dimension to
    # the desired chunk size, for example: `{"time": 15}`
}

recipe = XarrayZarrRecipe(pattern, target_chunks=chunks)