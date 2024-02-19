import apache_beam as beam
import zarr
from pangeo_forge_recipes.patterns import FilePattern
from pangeo_forge_recipes.transforms import (
    Indexed,
    OpenURLWithFSSpec,
    OpenWithXarray,
    StoreToZarr,
    T,
)

def make_url():
    return (
        "https://hs.pangaea.de/Maps/antgg2015/antgg2015.nc"
    )

pattern = FilePattern(make_url)

def test_ds(store: zarr.storage.FSStore) -> zarr.storage.FSStore:
    # This fails integration test if not imported here
    import xarray as xr

    ds = xr.open_dataset(store, engine="zarr", consolidated=True, chunks={})
    for var in [
        "ellipsoidal_height",
        "orthometric_height",
        "free_air_anomaly",
        "accuracy_measure",
        "bouguer_anomaly",
        ]:
        assert var in ds.data_vars

class Preprocess(beam.PTransform):
    """Preprocessor transform."""

    @staticmethod
    def _preproc(item: Indexed[T]) -> Indexed[T]:
        import numpy as np

        index, ds = item
        ds["x"] = ds.x * 1000
        ds["y"] = ds.y * 1000
        ds = ds.drop(["longitude","latitude","crs"])
        ds = ds.expand_dims(time=np.array([time]))
        return index, ds

    def expand(self, pcoll: beam.PCollection) -> beam.PCollection:
        return pcoll | beam.Map(self._preproc)

transforms = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(
        file_type=pattern.file_type,
    )
    | Preprocess()
    | StoreToZarr(
        store_name="antgg2015.zarr",
    )
    | ConsolidateDimensionCoordinates()
    | ConsolidateMetadata()
    | "Test dataset" >> beam.Map(test_ds)
)