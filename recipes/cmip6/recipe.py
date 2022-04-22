import sys
import logging
import tempfile

from fsspec.implementations.local import LocalFileSystem
from pangeo_forge_recipes.storage import FSSpecTarget, CacheFSSpecTarget, MetadataTarget
from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.recipes import XarrayZarrRecipe

from esgf import (
    esgf_search,
)  # We probably want to strip this out later, left as is for now.

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


instance_ids = [
    'CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Omon.zos.gn.v20190429',
    'CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Omon.so.gn.v20190429',
    # 'CMIP6.CMIP.CCCma.CanESM5.historical.r1i1p1f1.Omon.thetao.gn.v20190429',
               ]


def recipe_from_urls(urls):
    pattern = pattern_from_file_sequence(urls, "time")

    recipe = XarrayZarrRecipe(
        pattern,
        target_chunks={"time": 3}, #TODO, this needs to be adjusted per dataset
        xarray_concat_kwargs={"join": "exact"},
    )
    return recipe


# TODO: ultimately we want this to work with a dictionary (waiting for this feature in pangeo cloud)
#recipes = {id:recipe_from_urls(urls_from_instance_id(id)) for id in instance_ids}

#but for now define them as explicit variables
recipe_0 = recipe_from_urls(urls_from_instance_id(instance_ids[0]))
recipe_1 = recipe_from_urls(urls_from_instance_id(instance_ids[1]))