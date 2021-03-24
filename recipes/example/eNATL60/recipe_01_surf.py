import pandas as pd
from pangeo_forge.recipe import NetCDFtoZarrSequentialRecipe
import os

months = pd.date_range("2010-02", "2010-05", freq="M")
region = 1

input_url_pattern = (
                "https://ige-meom-opendap.univ-grenoble-alpes.fr/"
                "thredds/fileServer/meomopendap/extract/SWOT-Adac/Surface/eNATL60/"
                "Region{reg}-surface-hourly_{yymm}.nc"
                     )
input_urls = [
              input_url_pattern.format(reg=os.path.join("%02d" % region),
                                       yymm=date.strftime("%Y-%m")
                                      )
              for date in months
             ]

recipe = NetCDFtoZarrSequentialRecipe(
                    input_urls=input_urls,
                    sequence_dim="time_counter",
                    inputs_per_chunk=20
                    )
