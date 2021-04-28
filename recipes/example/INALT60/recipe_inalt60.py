import numpy as np
from pangeo_forge.recipe import NetCDFtoZarrSequentialRecipe

def gen_url(freqs, dim, variables):
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
        for freq in freqs
        for var in variables
        for month in months
                 ]
    return input_urls


surf_ocean = NetCDFtoZarrSequentialRecipe(
                                input_urls=gen_url(['4h','5d'],'surface',['u','v','hts']),
                                sequence_dim="time_counter",
                                inputs_per_chunk=1,
                                nitems_per_input=None,
                                target_chunks={'time_counter': 15}
                                         )

surf_flux = NetCDFtoZarrSequentialRecipe(
                                input_urls=gen_url(['1d'],'surface',['flux','taux','tauy']),
                                sequence_dim="time_counter",
                                inputs_per_chunk=1,
                                nitems_per_input=None,
                                target_chunks={'time_counter': 15}
                                        )

int_ocean = NetCDFtoZarrSequentialRecipe(
                                input_urls=gen_url(['1d'],'upper1000m',['ts','u','v','w']),
                                sequence_dim="time_counter",
                                inputs_per_chunk=1,
                                nitems_per_input=None,
                                target_chunks={'time_counter': 15}
                                           )

grid = NetCDFtoZarrSequentialRecipe(
                                input_urls=(
                                "https://data.geomar.de/downloads/20.500.12085/"
                                "0e95d316-f1ba-47e3-b667-fc800afafe22/data/"
                                "INALT60_mesh_mask.nc"
                                           )
                                   )

recipe = {
        'surf_ocean':surf_ocean, 'surf_flux':surf_flux, 
        'int_ocean':int_ocean, 'grid':grid
         }
