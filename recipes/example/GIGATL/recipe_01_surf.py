import pandas as pd
from pangeo_forge.recipe import NetCDFtoZarrSequentialRecipe

dates = pd.date_range("2008-08-01", "2008-11-01", freq="5D")
region = 1
input_url_pattern = (
            "ftp://eftp.ifremer.fr/SWOT/"
            "gigatl1_1h_tides_surf_avg_{reg:1d}_{yymmdd}.nc"
                    )
input_urls = [
        input_url_pattern.format(reg=region,
                                 yymmdd=day.strftime("%Y-%m-%d")
                                )
        for day in dates
             ]

recipe = NetCDFtoZarrSequentialRecipe(
                            input_urls=input_urls,
                            sequence_dim="time",
                            inputs_per_chunk=1,
                            nitems_per_input=None,
                            target_chunks={"time": 24}
                                    )
