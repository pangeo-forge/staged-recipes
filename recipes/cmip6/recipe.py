############################# just copied from esfg.py (TODO: being able to import this, would be nice and clean) ############################################
"""ESGF API Search Results to Pandas Dataframes
"""
import requests
import numpy
import pandas as pd

#dummy comment
# copied from Naomis code https://github.com/pangeo-data/pangeo-cmip6-cloud/blob/master/myconfig.py
target_keys = [
    "activity_id",
    "institution_id",
    "source_id",
    "experiment_id",
    "member_id",
    "table_id",
    "variable_id",
    "grid_label",
]

target_format = "%(" + ")s/%(".join(target_keys) + ")s"


node_pref = {
    "esgf-data1.llnl.gov": 0,
    "esgf-data2.llnl.gov": 0,
    "aims3.llnl.gov": 0,
    "esgdata.gfdl.noaa.gov": 10,
    "esgf-data.ucar.edu": 10,
    "dpesgf03.nccs.nasa.gov": 5,
    "crd-esgf-drc.ec.gc.ca": 6,
    "cmip.bcc.cma.cn": 10,
    "cmip.dess.tsinghua.edu.cn": 10,
    "cmip.fio.org.cn": 10,
    "dist.nmlab.snu.ac.kr": 10,
    "esg-cccr.tropmet.res.in": 10,
    "esg-dn1.nsc.liu.se": 10,
    "esg-dn2.nsc.liu.se": 10,
    "esg.camscma.cn": 10,
    "esg.lasg.ac.cn": 10,
    "esg1.umr-cnrm.fr": 10,
    "esgf-cnr.hpc.cineca.it": 10,
    "esgf-data2.diasjp.net": 10,
    "esgf-data3.ceda.ac.uk": 10,
    "esgf-data3.diasjp.net": 10,
    "esgf-nimscmip6.apcc21.org": 10,
    "esgf-node2.cmcc.it": 10,
    "esgf.bsc.es": 10,
    "esgf.dwd.de": 10,
    "esgf.ichec.ie": 10,
    "esgf.nci.org.au": 10,
    "esgf.rcec.sinica.edu.tw": 10,
    "esgf3.dkrz.de": 10,
    "noresg.nird.sigma2.no": 10,
    "polaris.pknu.ac.kr": 10,
    "vesg.ipsl.upmc.fr": 10,
}


# Author: Unknown
# I got the original version from a word document published by ESGF
# https://docs.google.com/document/d/1pxz1Kd3JHfFp8vR2JCVBfApbsHmbUQQstifhGNdc6U0/edit?usp=sharing
# API AT:
# https://github.com/ESGF/esgf.github.io/wiki/ESGF_Search_REST_API#results-pagination


def esgf_search(
    search,
    server="https://esgf-node.llnl.gov/esg-search/search",
    files_type="HTTPServer",
    local_node=True,
    project="CMIP6",
    page_size=500,
    verbose=False,
    format="application%2Fsolr%2Bjson",
    toFilter=True,
):

    client = requests.session()
    payload = search
    payload["project"] = project
    payload["type"] = "File"
    if local_node:
        payload["distrib"] = "false"

    payload["format"] = format
    payload["limit"] = 500

    numFound = 10000
    all_frames = []
    offset = 0
    while offset < numFound:
        payload["offset"] = offset
        url_keys = []
        for k in payload:
            url_keys += ["{}={}".format(k, payload[k])]

        url = "{}/?{}".format(server, "&".join(url_keys))
        print(url)
        r = client.get(url)
        r.raise_for_status()
        resp = r.json()["response"]
        numFound = int(resp["numFound"])

        resp = resp["docs"]
        offset += len(resp)
        # print(offset,numFound,len(resp))
        for d in resp:
            dataset_id = d["dataset_id"]
            dataset_size = d["size"]
            for f in d["url"]:
                sp = f.split("|")
                if sp[-1] == files_type:
                    url = sp[0]
                    if sp[-1] == "OPENDAP":
                        url = url.replace(".html", "")
                    dataset_url = url
            all_frames += [[dataset_id, dataset_url, dataset_size]]

    ddict = {}
    item = 0
    for item, alist in enumerate(all_frames):
        dataset_id = alist[0]
        dataset_url = alist[1]
        dataset_size = alist[2]
        vlist = dataset_id.split("|")[0].split(".")[-9:]
        vlist += [dataset_url.split("/")[-1]]
        vlist += [dataset_size]
        vlist += [dataset_url]
        vlist += [dataset_id.split("|")[-1]]
        ddict[item] = vlist
        item += 1

    dz = pd.DataFrame.from_dict(ddict, orient="index")
    if len(dz) == 0:
        print("empty search response")
        return dz

    dz = dz.rename(
        columns={
            0: "activity_id",
            1: "institution_id",
            2: "source_id",
            3: "experiment_id",
            4: "member_id",
            5: "table_id",
            6: "variable_id",
            7: "grid_label",
            8: "version_id",
            9: "ncfile",
            10: "file_size",
            11: "url",
            12: "data_node",
        }
    )

    dz["ds_dir"] = dz.apply(lambda row: target_format % row, axis=1)
    dz["node_order"] = [node_pref[s] for s in dz.data_node]
    dz["start"] = [s.split("_")[-1].split("-")[0] for s in dz.ncfile]
    dz["stop"] = [s.split("_")[-1].split("-")[-1].split(".")[0] for s in dz.ncfile]

    if toFilter:
        # remove all 999 nodes
        dz = dz[dz.node_order != 999]

        # keep only best node
        dz = dz.sort_values(by=["node_order"])
        dz = dz.drop_duplicates(subset=["ds_dir", "ncfile", "version_id"], keep="first")

        # keep only most recent version from best node
        dz = dz.sort_values(by=["version_id"])
        dz = dz.drop_duplicates(subset=["ds_dir", "ncfile"], keep="last")

    return dz

####################################################
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

# from esgf import (
#     esgf_search,
# )  # We probably want to strip this out later, left as is for now.

node_dict = {
    "llnl": "https://esgf-node.llnl.gov/esg-search/search",
    "ipsl": "https://esgf-node.ipsl.upmc.fr/esg-search/search",
    "ceda": "https://esgf-index1.ceda.ac.uk/esg-search/search",
    "dkrz": "https://esgf-data.dkrz.de/esg-search/search",
}

def urls_from_instance_id(instance_id):
    # get facets from instance_id
    facet_labels = (
        "mip_era",
        "activity_id",
        "institution_id",
        "source_id",
        "experiment_id",
        "member_id",
        "table_id",
        "variable_id",
        "grid_label",
        "version",
    )

    facet_vals = instance_id.split(".")
    if len(facet_vals) != 10:
        raise ValueError(
            "Please specify a query of the form {"
            + ("}.{".join(facet_labels).upper())
            + "}"
        )

    facets = dict(zip(facet_labels, facet_vals))

    if facets["mip_era"] != "CMIP6":
        raise ValueError("Only CMIP6 mip_era supported")
        
    
    # version doesn't work here
    keep_facets = (
        "activity_id",
        "institution_id",
        "source_id",
        "experiment_id",
        "member_id",
        "table_id",
        "variable_id",
        "grid_label",
    )
    search_facets = {f: facets[f] for f in keep_facets}

    search_node = "llnl"
    ESGF_site = node_dict[
        search_node
    ]  # TODO: We might have to be more clever here and search through different nodes. For later.


    df = esgf_search(search_facets, server=ESGF_site)  # this modifies the dict inside?

    # get list of urls
    urls = df["url"].tolist()

    # sort urls in decending time order (to be able to pass them directly to the pangeo-forge recipe)
    end_dates = [url.split("-")[-1].replace(".nc", "") for url in urls]
    urls = [url for _, url in sorted(zip(end_dates, urls))]
    
    # version is still not working
    # if facets["version"].startswith("v"):
    #    facets["version"] = facets["version"][1:]

    # TODO Check that there are no gaps or duplicates.
    
    return urls

inputs = {
    'CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Omon.zos.gn.v20190429':{'target_chunks':{'time':360}},
    'CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Omon.so.gn.v20190429':{'target_chunks':{'time':6}, 'subset_inputs':{'time':5}},
}


def recipe_from_urls(urls, instance_kwargs):
    pattern = pattern_from_file_sequence(urls, "time")

    recipe = XarrayZarrRecipe(
        pattern,
        xarray_concat_kwargs={"join": "exact"},
        **instance_kwargs
        
    )
    return recipe

# TODO: ultimately we want this to work with a dictionary (waiting for this feature in pangeo cloud)
#recipes = {iid:recipe_from_urls(urls_from_instance_id(iid), kwargs) for iid,kwargs in inputs.items()}

#but for now define them as explicit variables
iid = 'CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Omon.zos.gn.v20190429'
recipe_0 = recipe_from_urls(urls_from_instance_id(iid), inputs[iid])

iid = 'CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Omon.so.gn.v20190429'
recipe_1 = recipe_from_urls(urls_from_instance_id(iid), inputs[iid])