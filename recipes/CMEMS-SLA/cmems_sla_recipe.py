import pandas as pd
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe


def make_full_path(time):
    """Return a valid ftp download url based on a date input
    """

    year, month, day = time.year, time.month, time.day

    return (
        "ftp://my.cmems-du.eu/Core/SEALEVEL_GLO_PHY_L4_REP_OBSERVATIONS_008_047/"
        f"dataset-duacs-rep-global-merged-allsat-phy-l4/{year}/{month:02d}/"
        f"dt_global_allsat_phy_l4_{year}{month:02d}{day:02d}_20190101.nc"
    )


dates = pd.date_range(start="1993-01-01", end="2017-05-15")

concat_dim = ConcatDim("time", keys=dates, nitems_per_file=1)

file_pattern = FilePattern(make_full_path, concat_dim)

chunks = {"time": 12}  # for the `"sla"` variable, this yields ~100 MB chunks

recipe = XarrayZarrRecipe(file_pattern, target_chunks=chunks)
