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


dims = np.array(['surface','upper1000m'])
for dim in dims:
    if dim == 'surface':
        for freq in np.array(['4h','1d','5d']):
            if freq == '1d':
                variables = np.array(['taux','tauy','flux'])
            else:
                variables = np.array(['u','v','hts'])
            recipe = NetCDFtoZarrSequentialRecipe(
                            input_urls=gen_url(freq,dim,variables),
                            sequence_dim="time_counter",
                            inputs_per_chunk=1,
                            nitems_per_input=None,
                            target_chunks={'time_counter': 15}
                                                 )
    else:
        freq = '1d'
        variables = np.array(['ts','u','v','w'])
        recipe = NetCDFtoZarrSequentialRecipe(
                            input_urls=input_urls,
                            sequence_dim="time_counter",
                            inputs_per_chunk=1,
                            nitems_per_input=None,
                            target_chunks={'time_counter': 15}
                                             )
