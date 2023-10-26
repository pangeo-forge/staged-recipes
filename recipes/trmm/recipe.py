import base64
import json
import os
from dataclasses import dataclass, field
from typing import Dict, Union

import apache_beam as beam
import requests
import zarr
from cmr import GranuleQuery
from kerchunk.combine import MultiZarrToZarr
from xarray import Dataset

from pangeo_forge_recipes.patterns import pattern_from_file_sequence
from pangeo_forge_recipes.storage import FSSpecTarget
from pangeo_forge_recipes.transforms import (
    CombineReferences,
    OpenWithKerchunk,
    RequiredAtRuntimeDefault,
    WriteCombinedReference,
)
from pangeo_forge_recipes.writers import ZarrWriterMixin

HTTP_REL = 'http://esipfed.org/ns/fedsearch/1.1/data#'
S3_REL = 'http://esipfed.org/ns/fedsearch/1.1/s3#'

# This recipe requires the following environment variables from Earthdata
ED_TOKEN = os.environ['EARTHDATA_TOKEN']
ED_USERNAME = os.environ['EARTHDATA_USERNAME']
ED_PASSWORD = os.environ['EARTHDATA_PASSWORD']

CREDENTIALS_API = 'https://data.gesdisc.earthdata.nasa.gov/s3credentials'
SHORT_NAME = 'TRMM_3B42_Daily'
CONCAT_DIM = 'time'
IDENTICAL_DIMS = ['lat', 'lon']

# use HTTP_REL if S3 access is not possible. S3_REL is faster.
selected_rel = S3_REL


def earthdata_auth(username: str, password: str):
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
    return filter(
        lambda link: link['rel'] == rel
        and (link['href'].endswith('.nc') or link['href'].endswith('.nc4')),
        links,
    )


def gen_data_links(rel):
    granules = GranuleQuery().short_name(SHORT_NAME).downloadable(True).get_all()
    for granule in granules:
        s3_links = filter_data_links(granule['links'], rel)
        first = next(s3_links, None)
        # throw if CMR does not have exactly one S3 link for an item
        if not first or next(s3_links, None) is not None:
            raise ValueError(f"Expected 1 link of type {rel} on {granule['title']}")
        yield first['href']


@dataclass
class ConsolidateMetadata(beam.PTransform):
    """Consolidate metadata into a single .zmetadata file.

    See zarr.consolidate_metadata() for details.
    """

    storage_options: Dict = field(default_factory=dict)

    @staticmethod
    def _consolidate(mzz: MultiZarrToZarr, storage_options: Dict) -> Dict:
        import fsspec
        import zarr
        from kerchunk.utils import consolidate

        out = mzz.translate()
        fs = fsspec.filesystem(
            'reference',
            fo=out,
            remote_options=storage_options,
        )
        mapper = fs.get_mapper()
        metadata_key = '.zmetadata'
        zarr.consolidate_metadata(mapper, metadata_key=metadata_key)
        double_consolidated = consolidate(dict([(metadata_key, mapper[metadata_key])]))
        out['refs'] = out['refs'] | double_consolidated['refs']
        return out

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._consolidate, storage_options=self.storage_options)


@dataclass
class WriteReferences(beam.PTransform, ZarrWriterMixin):
    """Store a singleton PCollection consisting of a ``kerchunk.combine.MultiZarrToZarr`` object.

    :param store_name: Zarr store will be created with this name under ``target_root``.
    :param output_file_name: Name to give the output references file
      (``.json`` or ``.parquet`` suffix).
    """

    store_name: str
    output_file_name: str = 'reference.json'
    target_root: Union[str, FSSpecTarget, RequiredAtRuntimeDefault] = field(
        default_factory=RequiredAtRuntimeDefault
    )
    storage_options: Dict = field(default_factory=dict)

    @staticmethod
    def _write(
        refs: Dict, full_target: FSSpecTarget, output_file_name: str, storage_options: Dict
    ) -> Dataset:
        import fsspec
        import ujson
        import xarray as xr

        outpath = full_target._full_path(output_file_name)
        with full_target.fs.open(outpath, 'wb') as f:
            f.write(ujson.dumps(refs).encode())

        fs = fsspec.filesystem(
            'reference', fo=full_target._full_path(output_file_name), remote_options=storage_options
        )
        return xr.open_dataset(
            fs.get_mapper(), engine='zarr', backend_kwargs={'consolidated': True}
        )

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(
            self._write,
            full_target=self.get_full_target(),
            output_file_name=self.output_file_name,
            storage_options=self.storage_options,
        )


@dataclass
class ValidateDatasetDimensions(beam.PTransform):
    """Open the reference.json in xarray and validate dimensions."""

    expected_dims: Dict = field(default_factory=dict)

    @staticmethod
    def _validate(ds: Dataset, expected_dims: Dict) -> None:
        if set(ds.dims) != expected_dims.keys():
            raise ValueError(f'Expected dimensions {expected_dims.keys()}, got {ds.dims}')
        for dim, bounds in expected_dims.items():
            if bounds is None:
                continue
            lo, hi = bounds
            actual_lo, actual_hi = round(ds[dim].data.min()), round(ds[dim].data.max())
            if actual_lo != lo or actual_hi != hi:
                raise ValueError(f'Expected {dim} range [{lo}, {hi}], got {actual_lo, actual_hi}')
        return ds

    def expand(
        self,
        pcoll: beam.PCollection,
    ) -> beam.PCollection:
        return pcoll | beam.Map(self._validate, expected_dims=self.expected_dims)


fsspec_auth_kwargs = (
    {'headers': {'Authorization': f'Bearer {ED_TOKEN}'}}
    if selected_rel == HTTP_REL
    else {'client_kwargs': earthdata_auth(ED_USERNAME, ED_PASSWORD)}
)
pattern = pattern_from_file_sequence(
    list(gen_data_links(selected_rel)), CONCAT_DIM, fsspec_open_kwargs=fsspec_auth_kwargs
)

# target_root is injected only into certain transforms in pangeo-forge-recipes
# this is a hacky way to pull it out of the WriteCombinedReference transform
hacky_way_to_pull = WriteCombinedReference(
    store_name=SHORT_NAME,
    concat_dims=pattern.concat_dims,
    identical_dims=IDENTICAL_DIMS,
)


def get_time_from_attr(index: int, z: zarr.Group, var: str, fn: str):
    import numpy as np

    return np.datetime64(z.attrs['BeginDate'])


recipe = (
    beam.Create(pattern.items())
    | OpenWithKerchunk(
        remote_protocol='s3' if selected_rel == S3_REL else 'https',
        file_type=pattern.file_type,
        storage_options=pattern.fsspec_open_kwargs,
    )
    | CombineReferences(
        concat_dims=pattern.concat_dims,
        identical_dims=IDENTICAL_DIMS,
        mzz_kwargs={'coo_map': {'time': get_time_from_attr}},
    )
    | ConsolidateMetadata(storage_options=pattern.fsspec_open_kwargs)
    | WriteReferences(
        store_name=SHORT_NAME,
        target_root=hacky_way_to_pull.target_root,
        storage_options=pattern.fsspec_open_kwargs,
    )
    | ValidateDatasetDimensions(expected_dims={'time': None, 'lat': (-50, 50), 'lon': (-180, 180)})
)
