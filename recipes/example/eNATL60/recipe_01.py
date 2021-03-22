import pandas as pd
from pangeo_forge.recipe import NetCDFtoZarrSequentialRecipe
import os

dates = pd.date_range("2021-02", "2021-05", freq="M")
region = 1

input_url_pattern = (
                "https://ige-meom-opendap.univ-grenoble-alpes.fr/"
                "thredds/fileServer/meomopendap/extract/SWOT-Adac/Surface/eNATL60/"
                "Region{reg}-surface-hourly_{month}.nc"
                     )
input_urls = [
              input_url_pattern.format(reg=os.path.join("%02d" % region),
                                       month=day.strftime("%Y%m")
                                      )
              for day in dates
             ]

recipe = NetCDFtoZarrSequentialRecipe(
                    input_urls=input_urls,
                    sequence_dim="time_counter",
                    inputs_per_chunk=20
                    )
