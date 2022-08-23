from pangeo_forge_recipes.recipes import XarrayZarrRecipe
from pangeo_forge_recipes.patterns import FilePattern, ConcatDim, MergeDim

# Make FilePattern

# Include all 41 years and the variables pertinent to air-sea flux calculations
years = list(range(46,87))
variables = [
    "LHFLX",
    "SHFLX",
    "FLDS",
    "FSNS",
    "PSL",
    "TAUX",
    "TAUY",
    "TS",
    "U10"
]

def make_filename(variable, time):
    return ("https://tds.ucar.edu/thredds/fileServer/datazone/campaign/cesm/collections/ASD/"
            "hybrid_v5_rel04_BC5_ne120_t12_pop62/atm-regrid/proc/tseries/daily/FV_768x1152.bilinear."
            f"hybrid_v5_rel04_BC5_ne120_t12_pop62.cam.h1.{variable}.00{time}0101-00{time}1231.nc")
            
            
pattern = FilePattern(
    make_filename,
    ConcatDim(name="time", keys=years),
    MergeDim(name="variable", keys=variables)
)

# Define the recipe

target_chunks = {"time": 73} # Full spatial domain: "lat": 768, "lon": 1152, --> if not specified, will include full extent

recipe = XarrayZarrRecipe(
            file_pattern=pattern,
            target_chunks=target_chunks,
            subset_inputs = {"time": 5}) # set 5 chunks per year, each with time of length 73 (to total 365)