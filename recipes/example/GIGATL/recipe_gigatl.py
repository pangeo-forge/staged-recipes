import pandas as pd
from pangeo_forge.recipe import NetCDFtoZarrSequentialRecipe

dates = pd.date_range("2008-08-01", "2008-11-01", freq="5D")
dates = pd.concat([dates, pd.date_range("2009-02-01", "2009-05-01", freq="5D")
                  ], ignore_index=True)

def get_url(dates, region, dim='surf'):
    input_url_pattern = (
            "ftp://eftp.ifremer.fr/SWOT/"
            "gigatl1_1h_tides_{dim}_avg_{reg:1d}_{yymmdd}.nc"
                        )
    input_urls = [
        input_url_pattern.format(
                            dim=dim,
                            reg=region,
                            yymmdd=day.strftime("%Y-%m-%d")
                                )
        for day in dates
                 ]

surf_01 = NetCDFtoZarrSequentialRecipe(
                            input_urls=get_url(dates, 1),
                            sequence_dim="time",
                            inputs_per_chunk=1,
                            nitems_per_input=None,
                            target_chunks={"time":24}
                                      )

surf_02 = NetCDFtoZarrSequentialRecipe(
                            input_urls=get_url(dates, 2)
                            sequence_dim="time",
                            inputs_per_chunk=1,
                            nitems_per_input=None,
                            target_chunks={"time":24}
                                      )

recipe = {'surf-reg-01':surf_01, 'surf-reg-02':surf_02}
