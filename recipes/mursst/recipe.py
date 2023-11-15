import base64
import json
import os

import apache_beam as beam
import requests
from cmr import GranuleQuery

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.transforms import (
    Indexed,
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    T,
)

HTTP_REL = 'http://esipfed.org/ns/fedsearch/1.1/data#'
S3_REL = 'http://esipfed.org/ns/fedsearch/1.1/s3#'
AUTH_HEADERS = {'headers': {'Authorization': f"Bearer {os.environ['EARTHDATA_TOKEN']}"}}
CREDENTIALS_API = 'https://archive.podaac.earthdata.nasa.gov/s3credentials'


def earthdata_auth(username, password):
    login_resp = requests.get(CREDENTIALS_API, allow_redirects=False)
    login_resp.raise_for_status()

    encoded_auth = base64.b64encode(f'{username}:{password}'.encode('ascii'))
    auth_redirect = requests.post(
        login_resp.headers['location'],
        data={'credentials': encoded_auth},
        headers={'Origin': CREDENTIALS_API},
        allow_redirects=False,
    )
    auth_redirect.raise_for_status()

    final = requests.get(auth_redirect.headers['location'], allow_redirects=False)

    results = requests.get(CREDENTIALS_API, cookies={'accessToken': final.cookies['accessToken']})
    results.raise_for_status()

    creds = json.loads(results.content)
    return {
        'aws_access_key_id': creds['accessKeyId'],
        'aws_secret_access_key': creds['secretAccessKey'],
        'aws_session_token': creds['sessionToken'],
    }


def filter_data_links(links, rel):
    return filter(lambda link: link['rel'] == rel and link['href'].endswith('.nc'), links)


def gen_data_links(rel):
    granules = GranuleQuery().short_name('MUR-JPL-L4-GLOB-v4.1').downloadable(True).get_all()
    for granule in granules:
        s3_links = filter_data_links(granule['links'], rel)
        first = next(s3_links, None)
        # throw if CMR does not have exactly one S3 link for an item
        if not first or next(s3_links, None) is not None:
            raise ValueError(f"Expected 1 link of type {rel} on {granule['title']}")
        yield first['href']


class Preprocess(beam.PTransform):
    """Filters variables to only be the non-optional L4 variables."""

    @staticmethod
    def _preproc(item: Indexed[T]) -> Indexed[T]:
        SELECTED_VARS = {'analysed_sst', 'analysis_error', 'mask', 'sea_ice_fraction'}
        index, ds = item
        return index, ds.drop([k for k in ds.data_vars.keys() if k not in SELECTED_VARS])

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._preproc)


# use HTTP_REL if S3 access is not possible. S3_REL is faster.
selected_rel = S3_REL
pattern = pattern_from_file_sequence(
    list(gen_data_links(selected_rel)),
    'time',
)
open_kwargs = (
    AUTH_HEADERS
    if selected_rel == HTTP_REL
    else {
        'client_kwargs': earthdata_auth(
            os.environ['EARTHDATA_USERNAME'], os.environ['EARTHDATA_PASSWORD']
        )
    }
)
recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec(open_kwargs=open_kwargs)
    | OpenWithXarray(file_type=pattern.file_type)
    | Preprocess()
    | StoreToZarr(
        store_name='mursst.zarr',
        combine_dims=pattern.combine_dim_keys,
        target_chunks={'time': 1, 'lat': 1800, 'lon': 3600},
    )
)
