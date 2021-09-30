"""Author: Norland Raphael Hagen - 08-03-2021
Pangeo-Forge recipe for cr2met data (cr2met v2 1979-2020: Chilean Center for Climate and Resilience Research - Gridded daily precip and temperature. )
"""  # noqa: E501


from pangeo_forge_recipes.patterns import ConcatDim, FilePattern, MergeDim
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# Filename Pattern Inputs
target_chunks = {"lat": 800, "lon": 220, "time": 15096}

variables = ["pr", "t2m", "tmax", "tmin"]


def make_filename(time, variable):
    if variable == "pr":
        fname = "https://www.cr2.cl/download/cr2met_v2-0_pr_day_1979_2020/?wpdmdl=28866&ind=XEBr5cgJaEiPZ1OFhMQLzsGEQ2nIIGyaUXHZlZqrWmCl8TFr4qSxI2eBWBXizHHeDZtpy7gsohOq0wPs20kBmsAZNDWjlaaT4SVwXpop6zGrOAOfHBGIo2U59eNpOjT7AuxDSkAuTBTvDIrDXFvDzg"  # noqa: E501
    elif variable == "t2m":
        fname = "https://www.cr2.cl/download/cr2met_v2-0_t2m_day_1979_2020/?wpdmdl=28864&ind=l5tlDuy3dq_CWqbJ-K3jvoxzm77YdYE7Nph_YyQ0A6j_scgZ-kaugoW2ox85O5hyrpnL0_OOwxFWr7LoODrNgB_F4Jg-7qaXpu_lWox8b9H6w6d7DrY_YJyRRzuU7SVMyCLKCJk-cxCZkcSalzRSPw"  # noqa: E501
    elif variable == "tmax":
        fname = "https://www.cr2.cl/download/cr2met_v2-0_tmax_day_1979_2020/?wpdmdl=28862&ind=FH-qiDSW-IDlWVbL97QIBIH9pJNC2zf1377t4DNb7arzaTLv8shTVbXZQf9RUmZtYRSpuadjVcWU9MppWLSDWDOvqoESBnsOzcB31o-14ETxSUppjjXDeqfawstEbah8fcIsg7Sj22RSpFvsVbwOzQ"  # noqa: E501
    elif variable == "tmin":
        fname = "https://www.cr2.cl/download/cr2met_v2-0_tmin_day-_1979_2020/?wpdmdl=28859&ind=TY0Apx4oPcU_XU_P4Tez5FMHTXgdcgQbyukVXiBT-0Sm9JsVwkTR7bS72tdh96ffXB2viQq8-sYBORa3OucO7dtGbckdXr5Dh-2O6ISVCW4NsKOBwSRv3h-wGW0aaSwJKpPTY6UXP9VdNk-y2_V8GQ"  # noqa: E501
    return fname


pattern = FilePattern(
    make_filename, ConcatDim("time", keys=[""]), MergeDim(name="variable", keys=variables)
)


# Recipe Inputs
recipe = XarrayZarrRecipe(
    file_pattern=pattern, target_chunks=target_chunks, subset_inputs={"time": 136}
)
