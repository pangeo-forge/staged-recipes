import pandas as pd
from pangeo_forge.recipe import NetCDFtoZarrSequentialRecipe

datess = pd.date_range("2008-08-01", "2008-11-01", freq="5D")
datess = pd.concat([datess, pd.date_range("2009-02-01", "2009-05-01", freq="5D")
                   ], ignore_index=True)

def get_surf_url(dates, region, dim='surf'):
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
    return input_urls


datesi = pd.date_range("2008-08-01", "2008-11-01", freq="1D")
datesi = pd.concat([datesi, pd.date_range("2009-02-01", "2009-05-01", freq="1D")
                   ], ignore_index=True)

def get_int_url(dates, region):
    input_url_pattern = (
            "sftpgula@draco.univ-brest.fr:/GIGATL1/SWOT/"
            "gigatl1_1h_tides_region_{reg:02d}_{yymmdd}.nc"
                        )
    input_urls = [
            input_url_pattern.format(
                            reg=region,
                            yymmdd=day.strftime("%Y-%m-%d")
                                    )
            for day in dates
                 ]
    return input_urls

surf_01 = NetCDFtoZarrSequentialRecipe(
                            input_urls=get_surf_url(datess, 1),
                            sequence_dim="time",
                            inputs_per_chunk=1,
                            nitems_per_input=None,
                            target_chunks={"time":24}
                                      )

surf_02 = NetCDFtoZarrSequentialRecipe(
                            input_urls=get_surf_url(datess, 2),
                            sequence_dim="time",
                            inputs_per_chunk=1,
                            nitems_per_input=None,
                            target_chunks={"time":24}
                                      )

int_01 = NetCDFtoZarrSequentialRecipe(
                            input_urls=get_int_url(datesi, 1),
                            sequence_dim="time"
                            inputs_per_chunk=1
                            ntimes_per_input=None
                            target_chunks={"time":10}
                                     )

int_02 = NetCDFtoZarrSequentialRecipe(
                            input_urls=get_int_url(datesi, 2),
                            sequence_dim="time",
                            inputs_per_chunk=1,
                            ntimes_per_input=None,
                            target_chunks={"time":10}
                                     )

recipe = {'surf-reg-01':surf_01, 'surf-reg-02':surf_02,
          'int_reg-01' :int_01,  'int-reg-02' :int_02}
