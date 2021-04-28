import numpy as np
from pangeo_forge.recipe import NetCDFtoZarrSequentialRecipe

def gen_url(reg, freq):
    months = np.array(['Feb','Mar','Apr','Aug','Sep','Oct'], dtype=str)
    input_url_pattern = (
                "ftp://ftp.hycom.org/pub/xbxu/ATLc0.02/SWOT_ADAC/"
                "HYCOM50_E043_{month}_{reg}_{freq}.nc"
                            )
    input_urls = [
                input_url_pattern.format(month=month,
                                       reg=regs, freq=freqs
                                        )
                for month in months
                 ]
    return input_urls


surf_01 = NetCDFtoZarrSequentialRecipe(
                    input_urls=gen_url('GS','hourly'),
                    sequence_dim="time",
                    inputs_per_chunk=1,
                    nitems_per_input=None,
                    target_chunks={'time': 24}
                                      )

surf_02 = NetCDFtoZarrSequentialRecipe(
                    input_urls=gen_url('GE','hourly'),
                    sequence_dim="time",
                    inputs_per_chunk=1,
                    nitems_per_input=None,
                    target_chunks={'time': 24}
                                      )

surf_03 = NetCDFtoZarrSequentialRecipe(
                    input_urls=get_url('MD','hourly'),
                    sequence_dim="time",
                    inputs_per_chunk=1,
                    nitems_per_input=None,
                    target_chunks={"time":24}
                                      )

int_01 = NetCDFtoZarrSequentialRecipe(
                    input_urls=gen_url('GS','daily'),
                    sequence_dim="time",
                    inputs_per_chunk=1,
                    nitems_per_input=None,
                    target_chunks={'time': 24}
                                     )

int_02 = NetCDFtoZarrSequentialRecipe(
                    input_urls=gen_url('GE','daily'),
                    sequence_dim="time",
                    inputs_per_chunk=1,
                    nitems_per_input=None,
                    target_chunks={'time': 24}
                                     )

int_03 = NetCDFtoZarrSequentialRecipe(
                    input_urls=gen_url('MD','daily'),
                    sequence_dim="time",
                    inputs_per_chunk=1,
                    nitems_per_input=None,
                    target_chunks={'time': 24}
                                     )

grid_01 = NetCDFtoZarrSequentialRecipe(
                    input_urls=(
                        "ftp://ftp.hycom.org/pub/xbxu/ATLc0.02/SWOT_ADAC/"
                        "HYCOM50_grid_GS.nc"
                               )
                                      )

grid_02 = NetCDFtoZarrSequentialRecipe(
                    input_urls=(
                        "ftp://ftp.hycom.org/pub/xbxu/ATLc0.02/SWOT_ADAC/"
                        "HYCOM50_grid_GE.nc"
                        )
                                      )

grid_03 = NetCDFtoZarrSequentialRecipe(
                    input_urls=(
                        "ftp://ftp.hycom.org/pub/xbxu/ATLc0.02/SWOT_ADAC/"
                        "HYCOM50_grid_MD.nc"
                        )
                                      )

recipe = {
          'surf_01':surf_01, 'surf_02':surf_02, 'surf_03':surf_03, 
          'int_01':int_01, 'int_02':int_02, 'int_03':int_03,
          'grid_01':grid_01, 'grid_02':grid_02, 'grid_03':grid_03
         }
