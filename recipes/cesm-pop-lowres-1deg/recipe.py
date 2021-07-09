from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


def make_full_path(variable, time):
    """Returns a valid path to the source files
    Parameters
    ----------
    variable: str
        A string representing each variable
    """
    return (
        f"https://tds.ucar.edu/thredds/fileServer/datazone/campaign/cesm/collections/ASD/"
        f"v5_rel04_BC5_ne30_g16/ocn/proc/tseries/daily/v5_rel04_BC5_ne30_g16.pop.h.nday1."
        f"{variable}.{time}.nc"
    )


vars = [
    "HMXL_2",
    "SFWF_2",
    "SHF_2",
    "SSH_2",
    "SSS",
    "SST",
    "SST2",
    "TAUX_2",
    "TAUY_2",
    "U1_1",
    "U2_2",
    "V1_1",
    "V2_2",
    "XMXL_2",
]

concat_dim = ConcatDim("time", keys=["00010101-01661231",])

merge_dim = MergeDim("variable", keys=vars)

pattern = FilePattern(make_full_path, concat_dim, merge_dim)

chunks = {"time": 200}

recipe = XarrayZarrRecipe(pattern, target_chunks=chunks)
