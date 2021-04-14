import numpy as np
from pangeo_forge.recipe import NetCDFtoZarrSequentialRecipe

def gen_url(freq, dim, variables):
    months = np.array([2,3,4,8,9,10], dtype=int)
    input_url_pattern = (
            "https://data.geomar.de/downloads/20.500.12085/"
            "0e95d316-f1ba-47e3-b667-fc800afafe22/data/"
            "INALT60_{freq}_{dim}_{var}_{month:02d}.nc"
                    )
    input_urls = [
        input_url_pattern.format(freq=freq, dim=dim,
                                 var=var, month=month
                                )
        for var in variables
        for month in months
                 ]
    return input_urls


surf_ocean_4h = NetCDFtoZarrSequentialRecipe(
                                input_urls=gen_url('4h','surface',np.array(['u','v','hts'])),
                                sequence_dim="time_counter",
                                inputs_per_chunk=1,
                                nitems_per_input=None,
                                target_chunks={'time_counter': 15}
                                            )

surf_ocean_5d = NetCDFtoZarrSequentialRecipe(
                                input_urls=gen_url('5d','surface',np.array(['u','v','hts'])),
                                sequence_dim="time_counter",
                                inputs_per_chunk=1,
                                nitems_per_input=None,
                                target_chunks={'time_counter': 15}
                                            )

surf_flux = NetCDFtoZarrSequentialRecipe(
                                input_urls=gen_url('1d','surface',np.array(['flux','taux','tauy'])),
                                sequence_dim="time_counter",
                                inputs_per_chunk=1,
                                nitems_per_input=None,
                                target_chunks={'time_counter': 15}
                                        )

int_ocean = NetCDFtoZarrSequentialRecipe(
                                input_urls=gen_url('1d','upper1000m',np.array(['ts','u','v','w'])),
                                sequence_dim="time_counter",
                                inputs_per_chunk=1,
                                nitems_per_input=None,
                                target_chunks={'time_counter': 15}
                                           )

grid = NetCDFtoZarrSequentialRecipe(
                                input_urls=input_url_pattern = (
                                                "https://data.geomar.de/downloads/20.500.12085/"
                                                "0e95d316-f1ba-47e3-b667-fc800afafe22/data/"
                                                "INALT60_mesh_mask.nc"
                                                                                                                                                          )
                                   )

recipe = {
        'surf_ocean_4h':surf_ocean_4h, 'surf_ocean_5d':surf_ocean_5d,
        'surf_flux':surf_flux, 'int_ocean':int_ocean, 'grid':grid
         }
