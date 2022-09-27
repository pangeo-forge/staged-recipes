# List of instance ids to bring to process
# Note that the version is for now ignored
# (the latest is always chosen) TODO: See if
# we can make this specific to the version
import asyncio

from pangeo_forge_esgf import generate_recipe_inputs_from_iids

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

iids = [
    # PMIP runs requested by @rebeccaherman1
    # https://github.com/pangeo-forge/cmip6-feedstock/issues/3
    'CMIP6.DAMIP.BCC.BCC-CSM2-MR.hist-aer.r1i1p1f1.Amon.pr.gn.v20190507',
    #  'CMIP6.DAMIP.BCC.BCC-CSM2-MR.hist-aer.r2i1p1f1.Amon.pr.gn.v20190507',
    #  'CMIP6.DAMIP.BCC.BCC-CSM2-MR.hist-aer.r3i1p1f1.Amon.pr.gn.v20190508',
    'CMIP6.DAMIP.CAS.FGOALS-g3.hist-aer.r1i1p1f1.Amon.pr.gn.v20200411',
    #  'CMIP6.DAMIP.CAS.FGOALS-g3.hist-aer.r2i1p1f1.Amon.pr.gn.v20200411',
    #  'CMIP6.DAMIP.CAS.FGOALS-g3.hist-aer.r3i1p1f1.Amon.pr.gn.v20200411',
    'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r10i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r10i1p2f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r11i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r11i1p2f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r12i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r12i1p2f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r13i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r13i1p2f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r14i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r14i1p2f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r15i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r15i1p2f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r1i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r1i1p2f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r2i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r2i1p2f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r3i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r3i1p2f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r4i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r4i1p2f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r5i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r5i1p2f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r6i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r6i1p2f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r7i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r7i1p2f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r8i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r8i1p2f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r9i1p1f1.Amon.pr.gn.v20190429',
    #  'CMIP6.DAMIP.CCCma.CanESM5.hist-aer.r9i1p2f1.Amon.pr.gn.v20190429',
    'CMIP6.DAMIP.CNRM-CERFACS.CNRM-CM6-1.hist-aer.r10i1p1f2.Amon.pr.gr.v20190308',
    #  'CMIP6.DAMIP.CNRM-CERFACS.CNRM-CM6-1.hist-aer.r1i1p1f2.Amon.pr.gr.v20190308',
    #  'CMIP6.DAMIP.CNRM-CERFACS.CNRM-CM6-1.hist-aer.r2i1p1f2.Amon.pr.gr.v20190308',
    #  'CMIP6.DAMIP.CNRM-CERFACS.CNRM-CM6-1.hist-aer.r3i1p1f2.Amon.pr.gr.v20190308',
    #  'CMIP6.DAMIP.CNRM-CERFACS.CNRM-CM6-1.hist-aer.r4i1p1f2.Amon.pr.gr.v20190308',
    #  'CMIP6.DAMIP.CNRM-CERFACS.CNRM-CM6-1.hist-aer.r5i1p1f2.Amon.pr.gr.v20190308',
    #  'CMIP6.DAMIP.CNRM-CERFACS.CNRM-CM6-1.hist-aer.r6i1p1f2.Amon.pr.gr.v20190308',
    #  'CMIP6.DAMIP.CNRM-CERFACS.CNRM-CM6-1.hist-aer.r7i1p1f2.Amon.pr.gr.v20190308',
    #  'CMIP6.DAMIP.CNRM-CERFACS.CNRM-CM6-1.hist-aer.r8i1p1f2.Amon.pr.gr.v20190308',
    #  'CMIP6.DAMIP.CNRM-CERFACS.CNRM-CM6-1.hist-aer.r9i1p1f2.Amon.pr.gr.v20190308',
    #  'CMIP6.DAMIP.CSIRO-ARCCSS.ACCESS-CM2.hist-aer.r1i1p1f1.Amon.pr.gn.v20201120',
    #  'CMIP6.DAMIP.CSIRO-ARCCSS.ACCESS-CM2.hist-aer.r2i1p1f1.Amon.pr.gn.v20201120',
    #  'CMIP6.DAMIP.CSIRO-ARCCSS.ACCESS-CM2.hist-aer.r3i1p1f1.Amon.pr.gn.v20201120',
    'CMIP6.DAMIP.CSIRO.ACCESS-ESM1-5.hist-aer.r1i1p1f1.Amon.pr.gn.v20200615',
    #  'CMIP6.DAMIP.CSIRO.ACCESS-ESM1-5.hist-aer.r2i1p1f1.Amon.pr.gn.v20200615',
    #  'CMIP6.DAMIP.CSIRO.ACCESS-ESM1-5.hist-aer.r3i1p1f1.Amon.pr.gn.v20200615',
    'CMIP6.DAMIP.IPSL.IPSL-CM6A-LR.hist-aer.r10i1p1f1.Amon.pr.gr.v20180914',
    #  'CMIP6.DAMIP.IPSL.IPSL-CM6A-LR.hist-aer.r1i1p1f1.Amon.pr.gr.v20180914',
    #  'CMIP6.DAMIP.IPSL.IPSL-CM6A-LR.hist-aer.r2i1p1f1.Amon.pr.gr.v20180914',
    #  'CMIP6.DAMIP.IPSL.IPSL-CM6A-LR.hist-aer.r3i1p1f1.Amon.pr.gr.v20180914',
    #  'CMIP6.DAMIP.IPSL.IPSL-CM6A-LR.hist-aer.r4i1p1f1.Amon.pr.gr.v20180914',
    #  'CMIP6.DAMIP.IPSL.IPSL-CM6A-LR.hist-aer.r5i1p1f1.Amon.pr.gr.v20180914',
    #  'CMIP6.DAMIP.IPSL.IPSL-CM6A-LR.hist-aer.r6i1p1f1.Amon.pr.gr.v20180914',
    #  'CMIP6.DAMIP.IPSL.IPSL-CM6A-LR.hist-aer.r7i1p1f1.Amon.pr.gr.v20180914',
    #  'CMIP6.DAMIP.IPSL.IPSL-CM6A-LR.hist-aer.r8i1p1f1.Amon.pr.gr.v20180914',
    #  'CMIP6.DAMIP.IPSL.IPSL-CM6A-LR.hist-aer.r9i1p1f1.Amon.pr.gr.v20180914',
    'CMIP6.DAMIP.MIROC.MIROC6.hist-aer.r10i1p1f1.Amon.pr.gn.v20201228',
    #  'CMIP6.DAMIP.MIROC.MIROC6.hist-aer.r1i1p1f1.Amon.pr.gn.v20190705',
    #  'CMIP6.DAMIP.MIROC.MIROC6.hist-aer.r2i1p1f1.Amon.pr.gn.v20190705',
    #  'CMIP6.DAMIP.MIROC.MIROC6.hist-aer.r3i1p1f1.Amon.pr.gn.v20190705',
    #  'CMIP6.DAMIP.MIROC.MIROC6.hist-aer.r4i1p1f1.Amon.pr.gn.v20201228',
    #  'CMIP6.DAMIP.MIROC.MIROC6.hist-aer.r5i1p1f1.Amon.pr.gn.v20201228',
    #  'CMIP6.DAMIP.MIROC.MIROC6.hist-aer.r6i1p1f1.Amon.pr.gn.v20201228',
    #  'CMIP6.DAMIP.MIROC.MIROC6.hist-aer.r7i1p1f1.Amon.pr.gn.v20201228',
    #  'CMIP6.DAMIP.MIROC.MIROC6.hist-aer.r8i1p1f1.Amon.pr.gn.v20201228',
    #  'CMIP6.DAMIP.MIROC.MIROC6.hist-aer.r9i1p1f1.Amon.pr.gn.v20201228',
    'CMIP6.DAMIP.MOHC.HadGEM3-GC31-LL.hist-aer.r1i1p1f3.Amon.pr.gn.v20190814',
    #  'CMIP6.DAMIP.MOHC.HadGEM3-GC31-LL.hist-aer.r2i1p1f3.Amon.pr.gn.v20190815',
    #  'CMIP6.DAMIP.MOHC.HadGEM3-GC31-LL.hist-aer.r3i1p1f3.Amon.pr.gn.v20190814',
    #  'CMIP6.DAMIP.MOHC.HadGEM3-GC31-LL.hist-aer.r4i1p1f3.Amon.pr.gn.v20190814',
    #  'CMIP6.DAMIP.MOHC.HadGEM3-GC31-LL.hist-aer.r5i1p1f3.Amon.pr.gn.v20211123',
    'CMIP6.DAMIP.MRI.MRI-ESM2-0.hist-aer.r1i1p1f1.Amon.pr.gn.v20190320',
    #  'CMIP6.DAMIP.MRI.MRI-ESM2-0.hist-aer.r2i1p1f1.Amon.pr.gn.v20200327',
    #  'CMIP6.DAMIP.MRI.MRI-ESM2-0.hist-aer.r3i1p1f1.Amon.pr.gn.v20190320',
    #  'CMIP6.DAMIP.MRI.MRI-ESM2-0.hist-aer.r4i1p1f1.Amon.pr.gn.v20200327',
    #  'CMIP6.DAMIP.MRI.MRI-ESM2-0.hist-aer.r5i1p1f1.Amon.pr.gn.v20190320',
    'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r1i1p1f1.Amon.pr.gn.v20180821',
    #  'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r1i1p1f2.Amon.pr.gn.v20191226',
    #  'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r1i1p3f1.Amon.pr.gn.v20191226',
    #  'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r2i1p1f1.Amon.pr.gn.v20180821',
    #  'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r2i1p1f2.Amon.pr.gn.v20191226',
    #  'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r2i1p3f1.Amon.pr.gn.v20191226',
    #  'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r3i1p1f1.Amon.pr.gn.v20180822',
    #  'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r3i1p1f2.Amon.pr.gn.v20191226',
    #  'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r3i1p3f1.Amon.pr.gn.v20191226',
    #  'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r4i1p1f1.Amon.pr.gn.v20180823',
    #  'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r4i1p1f2.Amon.pr.gn.v20191226',
    #  'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r4i1p3f1.Amon.pr.gn.v20191226',
    #  'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r5i1p1f1.Amon.pr.gn.v20180823',
    #  'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r5i1p1f2.Amon.pr.gn.v20191226',
    #  'CMIP6.DAMIP.NASA-GISS.GISS-E2-1-G.hist-aer.r5i1p3f1.Amon.pr.gn.v20191226',
    'CMIP6.DAMIP.NCAR.CESM2.hist-aer.r1i1p1f1.Amon.pr.gn.v20200206',
    #  'CMIP6.DAMIP.NCAR.CESM2.hist-aer.r3i1p1f1.Amon.pr.gn.v20200305',
    'CMIP6.DAMIP.NCC.NorESM2-LM.hist-aer.r1i1p1f1.Amon.pr.gn.v20190920',
    #  'CMIP6.DAMIP.NCC.NorESM2-LM.hist-aer.r2i1p1f1.Amon.pr.gn.v20190920',
    #  'CMIP6.DAMIP.NCC.NorESM2-LM.hist-aer.r3i1p1f1.Amon.pr.gn.v20190920',
    'CMIP6.DAMIP.NOAA-GFDL.GFDL-ESM4.hist-aer.r1i1p1f1.Amon.pr.gr1.v20180701',
]

recipe_inputs = asyncio.run(generate_recipe_inputs_from_iids(iids))

recipes = {}

for iid, recipe_input in recipe_inputs.items():
    urls = recipe_input.get('urls', None)
    pattern_kwargs = recipe_input.get('pattern_kwargs', {})
    recipe_kwargs = recipe_input.get('recipe_kwargs', {})

    pattern = pattern_from_file_sequence(urls, 'time', **pattern_kwargs)
    if urls is not None:
        recipes[iid] = XarrayZarrRecipe(
            pattern, xarray_concat_kwargs={'join': 'exact'}, **recipe_kwargs
        )
print('+++Failed iids+++')
print(list(set(iids) - set(recipes.keys())))
print('+++Successful iids+++')
print(list(recipes.keys()))
