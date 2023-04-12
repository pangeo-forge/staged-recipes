import pandas as pd
from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

def make_full_path(time):
    """Return a valid ftp download url based on a date input
    """

    year, month, day = time.year, time.month, time.day
    if (year < 2021):
        postedDate = '20210726'
    else:
        if (month < 8 ):
            postedDate = '20220120'
        else:
            postedDate = '20220422'
    return (
        "ftp://my.cmems-du.eu/Core/SEALEVEL_GLO_PHY_L4_MY_008_047/"
        f"cmems_obs-sl_glo_phy-ssh_my_allsat-l4-duacs-0.25deg_P1D/{year}/{month:02d}/"
        f"dt_global_allsat_phy_l4_{year}{month:02d}{day:02d}_{postedDate}.nc"
    )

# Replace with a userid/pw that you've set up with Copernicus
userID = ""
pw = ""

ftpCreds = {'username': userID, 'password': pw}

dates = pd.date_range(start="1993-01-01", end="2021-12-31")

concat_dim = ConcatDim("time", keys=dates, nitems_per_file=1)

file_pattern = FilePattern(make_full_path, concat_dim, fsspec_open_kwargs=ftpCreds)

recipe = XarrayZarrRecipe(file_pattern, inputs_per_chunk=2)

