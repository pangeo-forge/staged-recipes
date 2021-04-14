import pandas as pd
from pangeo_forge.recipe import NetCDFtoZarrSequentialRecipe

months = np.array([2,3,4,8,9,10], dtype=int)
freq = '1d'
variables = np.array(['u','v','taux'])
input_url_pattern = (
            "https://data.geomar.de/downloads/20.500.12085/"
            "0e95d316-f1ba-47e3-b667-fc800afafe22/data/"
            "INALT60_{freq}_surface_{var}_{month:02d}.nc"
                    )
input_urls = [
        input_url_pattern.format(freq=freq, var=var, month=month
                                )
        for var in variables
        for month in months
             ]

recipe = NetCDFtoZarrSequentialRecipe(
                            input_urls=input_urls,
                            sequence_dim="time_counter",
                            inputs_per_chunk=1,
                            nitems_per_input=None,
                            target_chunks={'time_counter': 72}
                                    )
