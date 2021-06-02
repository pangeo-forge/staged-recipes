import pandas as pd

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

surf_dates = pd.date_range("2008-08-01", "2008-11-01", freq="5D").to_series()
surf_dates = pd.concat(
    [surf_dates, pd.date_range("2009-01-28", "2009-05-01", freq="5D").to_series()],
    ignore_index=True,
)


def get_surf_url(region, dim="surf", dates=surf_dates):
    pattern = "ftp://eftp.ifremer.fr/SWOT/SURF/gigatl1_1h_tides_{dim}_avg_{reg:1d}_{yymmdd}.nc"
    input_urls = [
        pattern.format(dim=dim, reg=region, yymmdd=day.strftime("%Y-%m-%d")) for day in dates
    ]
    return input_urls


int_dates = pd.date_range("2008-08-01", "2008-11-01", freq="1D").to_series()
int_dates = pd.concat(
    [int_dates, pd.date_range("2009-02-01", "2009-05-01", freq="1D").to_series()], ignore_index=True
)


def get_int_url(region, dates=int_dates):
    pattern = (
        "sftpgula@draco.univ-brest.fr:/GIGATL1/SWOT/"
        "gigatl1_1h_tides_region_{reg:02d}_{yymmdd}.nc"
    )
    input_urls = [pattern.format(reg=region, yymmdd=day.strftime("%Y-%m-%d")) for day in dates]
    return input_urls


surf_patterns = {
    f"surf_reg_{i:02}": pattern_from_file_sequence(get_surf_url(i), concat_dim="time",)
    for i in range(1, 3)
}

surf_recipes = {
    list(surf_patterns)[i]: (
        XarrayZarrRecipe(surf_patterns[list(surf_patterns)[i]], target_chunks={"time": 24},)
    )
    for i in range(2)
}

int_patterns = {
    f"int_reg_{i:02}": pattern_from_file_sequence(get_int_url(i), concat_dim="time",)
    for i in range(1, 3)
}

int_recipes = {
    list(int_patterns)[i]: (
        XarrayZarrRecipe(int_patterns[list(int_patterns)[i]], target_chunks={"time": 10},)
    )
    for i in range(2)
}

recipes = {
    **surf_recipes,
    **int_recipes,
}
